"""碳配额集中竞价市场：场次管理、密封报价、统一撮合、集中结算与权限审计。

业务状态机
==========
场次（AuctionSession）::

    draft ──open──▶ open ──match──▶ matched ──settle──▶ settled
      │                │                  │
      └──cancel────────┴────cancel────────┘
                        ▼
                     cancelled（撮合后撤场须逐笔释放卖方交易占用）

报价（AuctionBid）::

    active ──cancel──▶ cancelled（开放期，无账本副作用）
    active ──match───▶ matched / partial / unmatched（单次撮合定终态）

成交单（AuctionTrade）::

    reserved（撮合即占用卖方自由可用配额）──settle──▶ settled
                                          └─cancel──▶ cancelled（释放占用）

统一价格（uniform-price）双向竞价撮合
====================================
- 买入报价按价格降序、时间升序（id 升序）排队，卖出报价按价格升序、时间升序排队；
- 在每个候选价格 p 上：可行成交量 = min(买方中报价 ≥ p 的报量合计,
  卖方中报价 ≥ 保留价且报价 ≤ p 的报量合计)；
- 取可行成交量最大的价格为出清候选；多档等成交量时，按国内集合竞价惯例
  先选“未匹配量最小”档，仍并列则取候选档均价；无可行量则本场不成交；
- 成交分配遵循价格-时间优先：买方按队列逐单吃单，卖方按队列供货；
- 卖出报量先按其自由可用配额封顶（不得超卖），同一买方不与本企业自成交。

并发安全
========
- 场次键锁（auction:<id>）串行化该场次的一切写操作；报价撤单只取场次键，
  撮合/结算/撤场额外取全部参与企业的账户键与清缴键，锁序
  ``account: < auction: < clear: < order:`` 按名排序加锁防死锁；
- 撮合占用与结算划转复用账本原子条件 UPDATE：``current ≥ frozen + reserved``，
  履约冻结与交易占用（订单 + 竞价）互不可挤占；
- 结算/撤场用“状态必须为前置状态”的条件 UPDATE 抢占场次行，
  并发结算只有一个事务成功，其余幂等返回，绝不重复划转；
- 场次建场与报价均支持幂等键；状态抢占失败即整体回滚。

回写闭环
========
成交后写配额账户与交易流水（auction_bid_reserve/auction_bid_release/
auction_reserve_release/auction_deliver_out/auction_deliver_in/auction_deficit_clear），
并在同一事务内调用清缴内核核销买方同年度履约缺口（先冻结核销、再用到账配额补缴）；
全部敏感操作与越权拒绝写入权限审计日志。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.ledger import (
    InsufficientBalanceError,
    account_lock_key,
    apply_ledger_delta,
    auction_session_key,
    company_clear_key,
    is_duplicate_submit,
    lock_row_for_write,
    locked_accounts,
    transactional,
)
from app.models.allowance import AllowanceAccount, AllowanceTransaction
from app.models.auction import (
    AuctionAuditLog,
    AuctionBid,
    AuctionSession,
    AuctionTrade,
)
from app.models.company import Company
from app.services.quota_service import settle_buyer_deficit_on_auction

# 场次状态
DRAFT = "draft"
OPEN = "open"
MATCHED = "matched"
SETTLED = "settled"
CANCELLED = "cancelled"

# 报价状态
BID_ACTIVE = "active"
BID_MATCHED = "matched"
BID_PARTIAL = "partial"
BID_UNMATCHED = "unmatched"
BID_CANCELLED = "cancelled"

# 成交单状态
TRADE_RESERVED = "reserved"
TRADE_SETTLED = "settled"
TRADE_CANCELLED = "cancelled"

_BUY = "buy"
_SELL = "sell"


@dataclass
class Operator:
    """审计操作人（API 层从登录态构造，service 层不感知 HTTP）。"""

    id: int | None
    username: str
    role: str
    ip: str = ""


def _today() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _num(value) -> float:
    return float(value) if value is not None else 0.0


def _round4(value) -> float:
    return round(float(value), 4)


class AuctionError(ValueError):
    """竞价业务规则不满足（场次/报价状态非法、越权、余额不足等）。"""


# --------------------------------------------------------------------------- #
# 审计
# --------------------------------------------------------------------------- #

def write_audit(
    db: Session,
    operator: Operator | None,
    action: str,
    *,
    target_type: str = "",
    target_id: int | None = None,
    session_id: int | None = None,
    detail: str = "",
    result: str = "success",
    commit: bool = False,
) -> AuctionAuditLog:
    """写入一条竞价权限审计日志。

    业务操作默认 ``commit=False``：审计日志与业务变更在同一事务提交（同生共死）；
    越权拒绝（access.denied）等没有业务事务的场景由调用方传 ``commit=True`` 立即落库。
    """
    log = AuctionAuditLog(
        operator_id=operator.id if operator else None,
        operator_name=operator.username if operator else "anonymous",
        operator_role=operator.role if operator else "",
        action=action,
        target_type=target_type,
        target_id=target_id,
        session_id=session_id,
        detail=(detail or "")[:500],
        result=result,
        ip=operator.ip if operator else "",
    )
    db.add(log)
    db.flush()
    if commit:
        db.commit()
        db.refresh(log)
    return log


# --------------------------------------------------------------------------- #
# 基础查询与校验
# --------------------------------------------------------------------------- #

def _get_session(db: Session, session_id: int) -> AuctionSession:
    session = db.get(AuctionSession, session_id)
    if session is None:
        raise AuctionError("竞价场次不存在")
    return session


def _get_bid(db: Session, bid_id: int) -> AuctionBid:
    bid = db.get(AuctionBid, bid_id)
    if bid is None:
        raise AuctionError("报价单不存在")
    return bid


def _get_account(db: Session, company_id: int, year: int) -> AllowanceAccount:
    account = (
        db.query(AllowanceAccount)
        .filter(AllowanceAccount.company_id == company_id, AllowanceAccount.year == year)
        .first()
    )
    if account is None:
        raise AuctionError(f"企业 {company_id} 的 {year} 年度配额账户不存在，请先完成配额分配")
    return account


def _company_name(db: Session, company_id: int) -> str:
    company = db.get(Company, company_id)
    return company.name if company else str(company_id)


def _gen_session_no(db: Session) -> str:
    count = db.query(AuctionSession).count()
    return f"AUC{datetime.utcnow().year}{count + 1:06d}"


def _gen_bid_no(db: Session) -> str:
    count = db.query(AuctionBid).count()
    return f"BID{count + 1:08d}"


def _gen_trade_no(db: Session) -> str:
    count = db.query(AuctionTrade).count()
    return f"AT{count + 1:08d}"


def _transit_session(
    db: Session,
    session_id: int,
    expected: tuple[str, ...],
    new_status: str,
) -> int:
    """场次状态条件 UPDATE：仅前置状态命中时流转，返回影响行数。

    多进程部署下进程锁无法互斥时，由数据库行更新做最后抢占，
    杜绝并发撮合/结算/撤场造成重复占用、重复划转。
    """
    result = db.execute(
        update(AuctionSession)
        .where(AuctionSession.id == session_id)
        .where(AuctionSession.status.in_(expected))
        .values(status=new_status)
        .execution_options(synchronize_session=False)
    )
    return result.rowcount


def _add_ledger(
    db: Session,
    account: AllowanceAccount,
    tx_type: str,
    amount: float,
    balance_after: float,
    frozen_after: float,
    reserved_after: float,
    counterparty: str,
    remark: str,
    trade: AuctionTrade,
    tx_date: str,
) -> AllowanceTransaction:
    tx = AllowanceTransaction(
        account_id=account.id,
        company_id=account.company_id,
        tx_type=tx_type,
        amount=round(amount, 4),
        counterparty=counterparty,
        price=round(float(trade.price), 2),
        tx_date=tx_date,
        balance_after=round(balance_after, 4),
        frozen_after=round(frozen_after, 4),
        reserved_after=round(reserved_after, 4),
        auction_trade_id=trade.id,
        remark=remark,
    )
    db.add(tx)
    return tx


def _add_ledger_no_trade(
    db: Session,
    account: AllowanceAccount,
    tx_type: str,
    amount: float,
    balance_after: float,
    frozen_after: float,
    reserved_after: float,
    counterparty: str,
    remark: str,
    tx_date: str,
) -> AllowanceTransaction:
    """报价占用/释放类流水（尚无成交单，不写 price/auction_trade_id）。"""
    tx = AllowanceTransaction(
        account_id=account.id,
        company_id=account.company_id,
        tx_type=tx_type,
        amount=round(amount, 4),
        counterparty=counterparty,
        price=None,
        tx_date=tx_date,
        balance_after=round(balance_after, 4),
        frozen_after=round(frozen_after, 4),
        reserved_after=round(reserved_after, 4),
        remark=remark,
    )
    db.add(tx)
    return tx


# --------------------------------------------------------------------------- #
# 场次生命周期（监管）
# --------------------------------------------------------------------------- #

def create_session(
    db: Session,
    *,
    year: int,
    name: str,
    reserve_price: float = 0.0,
    estimated_volume: float | None = None,
    product: str = "allowance",
    auto_clear_deficit: bool = True,
    remark: str = "",
    open_at: datetime | None = None,
    close_at: datetime | None = None,
    operator: Operator | None = None,
    idempotency_key: str | None = None,
) -> AuctionSession:
    """监管创建竞价场次（草稿）。携带相同幂等键的重复提交返回首场。

    传入 open_at 时创建即直接开放（监管可一步建场）。
    """
    if reserve_price < 0:
        raise AuctionError("保留价不能为负数")
    if estimated_volume is not None and float(estimated_volume) < 0:
        raise AuctionError("拟成交量不能为负数")
    if product not in ("allowance", "CCER"):
        raise AuctionError("品种标识非法")

    # 建场无既有行可锁：幂等由唯一约束（幂等键 / session_no）与数据库事务兜底
    if idempotency_key:
        existing = (
            db.query(AuctionSession)
            .filter(AuctionSession.idempotency_key == idempotency_key)
            .first()
        )
        if existing:
            return existing
    try:
        with transactional(db):
            direct_open = open_at is not None
            session = AuctionSession(
                session_no=_gen_session_no(db),
                name=(name or "").strip()[:128],
                year=year,
                product=product,
                reserve_price=round(reserve_price, 2),
                estimated_volume=round(float(estimated_volume), 4) if estimated_volume is not None else None,
                status=OPEN if direct_open else DRAFT,
                auto_clear_deficit=1 if auto_clear_deficit else 0,
                open_at=open_at if direct_open else None,
                close_at=close_at,
                created_by=operator.id if operator else None,
                remark=(remark or "").strip()[:256],
                idempotency_key=idempotency_key,
            )
            db.add(session)
            db.flush()
            write_audit(
                db, operator,
                "session.open" if direct_open else "session.create",
                target_type="session", target_id=session.id, session_id=session.id,
                detail=f"创建竞价场次 {session.session_no}（{year}年度，保留价 {reserve_price}）"
                + ("，直接开放报价" if direct_open else ""),
            )
            db.refresh(session)
    except IntegrityError as exc:
        if idempotency_key and is_duplicate_submit(exc):
            db.rollback()
            existing = (
                db.query(AuctionSession)
                .filter(AuctionSession.idempotency_key == idempotency_key)
                .first()
            )
            if existing:
                return existing
        raise
    return session


def open_session(db: Session, session_id: int, operator: Operator | None) -> AuctionSession:
    """监管开放场次报价。仅草稿可开放；重复开放幂等。"""
    with locked_accounts([auction_session_key(session_id)]):
        session = _get_session(db, session_id)
        if session.status == OPEN:
            db.refresh(session)
            return session
        if session.status != DRAFT:
            raise AuctionError(f"场次当前为 {session.status}，不能开放报价")
        with transactional(db):
            if _transit_session(db, session_id, (DRAFT,), OPEN) != 1:
                raise AuctionError("场次状态已变化，开放失败，请刷新后重试")
            session = _get_session(db, session_id)
            session.open_at = datetime.utcnow()
            write_audit(
                db, operator, "session.open",
                target_type="session", target_id=session_id, session_id=session_id,
                detail=f"开放场次 {session.session_no} 报价",
            )
            db.flush()
            db.refresh(session)
        return session


def cancel_session(
    db: Session,
    session_id: int,
    operator: Operator | None,
    reason: str = "",
) -> AuctionSession:
    """监管撤场。

    - draft 撤场：无报价/无账本副作用；
    - open 撤场：active 卖出报价逐笔释放报价占用（auction_bid_release），
      全部报价置 cancelled；
    - matched 撤场：逐笔释放成交单占用（auction_reserve_release），
      成交单置 cancelled，已撮合报价回到 cancelled；
    - settled 场次终态不可撤。
    """
    reason = (reason or "").strip()[:256]
    with locked_accounts([auction_session_key(session_id)]):
        session = _get_session(db, session_id)
        if session.status == CANCELLED:
            db.refresh(session)
            return session
        if session.status == SETTLED:
            raise AuctionError("场次已结算，不能撤销")
        if session.status not in (DRAFT, OPEN, MATCHED):
            raise AuctionError(f"场次状态 {session.status} 不可撤销")

        # 需要释放占用的卖方账户：
        # - MATCHED：尚有 reserved 成交单的卖方；
        # - OPEN：仍有 active 卖出报价（报价时已占用）的卖方。
        trades: list[AuctionTrade] = []
        active_sell_bids: list[AuctionBid] = []
        if session.status == MATCHED:
            trades = (
                db.query(AuctionTrade)
                .filter(AuctionTrade.session_id == session_id, AuctionTrade.status == TRADE_RESERVED)
                .all()
            )
        elif session.status == OPEN:
            active_sell_bids = (
                db.query(AuctionBid)
                .filter(
                    AuctionBid.session_id == session_id,
                    AuctionBid.status == BID_ACTIVE,
                    AuctionBid.side == _SELL,
                )
                .all()
            )

        release_company_ids = {t.seller_id for t in trades} | {b.company_id for b in active_sell_bids}
        keys: list[str] = []
        accounts: dict[int, AllowanceAccount] = {}
        for cid in release_company_ids:
            acc = _get_account(db, cid, session.year)
            accounts[cid] = acc
            keys.append(account_lock_key(acc.id))
            keys.append(company_clear_key(cid, session.year))
        # 场次键已持有（外层），账户/清缴键按序加锁防死锁
        with locked_accounts(sorted(set(keys))):
            try:
                with transactional(db):
                    if _transit_session(db, session_id, (DRAFT, OPEN, MATCHED), CANCELLED) != 1:
                        raise AuctionError("场次状态已变化，撤场失败，请刷新后重试")
                    tx_date = _today()

                    if session.status == MATCHED and trades:
                        for trade in trades:
                            seller_acc = lock_row_for_write(db, accounts[trade.seller_id].id)
                            accounts[trade.seller_id] = seller_acc
                            qty = _round4(trade.quantity)
                            bal, frz, rsv = apply_ledger_delta(db, seller_acc.id, 0, 0, -qty)
                            buyer_name = _company_name(db, trade.buyer_id)
                            _add_ledger(
                                db, seller_acc, "auction_reserve_release", qty,
                                bal, frz, rsv, buyer_name,
                                f"场次 {session.session_no} 撤场，释放成交单 {trade.trade_no} 占用 {qty} 吨",
                                trade, tx_date,
                            )
                            trade.status = TRADE_CANCELLED
                            trade.cancelled_at = datetime.utcnow()

                        # 已撮合报价随撤场回到取消
                        db.execute(
                            update(AuctionBid)
                            .where(
                                AuctionBid.session_id == session_id,
                                AuctionBid.status.in_([BID_MATCHED, BID_PARTIAL]),
                            )
                            .values(
                                status=BID_CANCELLED,
                                cancel_reason="场次撤场，占用配额已释放",
                                cancelled_at=datetime.utcnow(),
                            )
                            .execution_options(synchronize_session=False)
                        )
                        # 无成交（unmatched）报价无需释放占用（撮合时已释放），仅标记取消
                        db.execute(
                            update(AuctionBid)
                            .where(
                                AuctionBid.session_id == session_id,
                                AuctionBid.status == BID_UNMATCHED,
                            )
                            .values(
                                status=BID_CANCELLED,
                                cancel_reason="场次撤场",
                                cancelled_at=datetime.utcnow(),
                            )
                            .execution_options(synchronize_session=False)
                        )

                    if session.status == OPEN:
                        # 释放每张有效卖出报价在报价时占用的配额
                        for bid in active_sell_bids:
                            acc = lock_row_for_write(db, accounts[bid.company_id].id)
                            accounts[bid.company_id] = acc
                            qty = _num(bid.quantity)
                            bal, frz, rsv = apply_ledger_delta(db, acc.id, 0, 0, -qty)
                            _add_ledger_no_trade(
                                db, acc, "auction_bid_release", qty,
                                bal, frz, rsv, "竞价撤场",
                                f"场次 {session.session_no} 撤场，释放卖出报价 {bid.bid_no} 占用 {qty} 吨",
                                tx_date,
                            )

                    # 开放期全部 active 报价（买/卖）直接置撤单
                    db.execute(
                        update(AuctionBid)
                        .where(
                            AuctionBid.session_id == session_id,
                            AuctionBid.status == BID_ACTIVE,
                        )
                        .values(
                            status=BID_CANCELLED,
                            cancel_reason=reason or "场次取消",
                            cancelled_at=datetime.utcnow(),
                        )
                        .execution_options(synchronize_session=False)
                    )

                    session = _get_session(db, session_id)
                    session.cancel_reason = reason
                    session.cancelled_by = operator.id if operator else None
                    session.cancelled_at = datetime.utcnow()
                    write_audit(
                        db, operator, "session.cancel",
                        target_type="session", target_id=session_id, session_id=session_id,
                        detail=f"撤销场次 {session.session_no}：{reason or '（无原因）'}"
                        + (f"，释放成交单 {len(trades)} 笔" if trades else "")
                        + (f"，释放有效卖出报价 {len(active_sell_bids)} 张" if active_sell_bids else ""),
                    )
                    db.flush()
                    db.refresh(session)
            except InsufficientBalanceError:
                raise AuctionError("释放竞价占用失败，账本状态异常，撤场已回滚")
            return session


# --------------------------------------------------------------------------- #
# 报价与撤单（买/卖方企业，或监管代操作）
# --------------------------------------------------------------------------- #

def place_bid(
    db: Session,
    session_id: int,
    company_id: int,
    side: str,
    quantity: float,
    price: float,
    *,
    tx_date: str = "",
    remark: str = "",
    operator: Operator | None = None,
    idempotency_key: str | None = None,
) -> AuctionBid:
    """在开放场次内提交密封报价。

    - side=sell 卖出：报量立即从自由可用配额转为交易占用（reserved），
      与企业间订单占用同一套账本不变量，杜绝“报价后配额被他用”的超卖竞态；
      撤单/未成交/部分成交余量按对应流程释放，成交部分结算时出库；
    - side=buy 买入：无配额校验（资金侧不在本台账范围）；
    - 同一企业同一场次同一方向只允许一张有效报价（部分唯一索引 + 键锁兜底）；
    - 报价价格不得低于场次保留价（买卖双方均以保留价为有效报价下限）。
    """
    if side not in (_BUY, _SELL):
        raise AuctionError("报价方向非法（buy/sell）")
    if quantity <= 0:
        raise AuctionError("报价数量必须为正数")
    if price < 0:
        raise AuctionError("报价单价不能为负数")
    if not db.get(Company, company_id):
        raise AuctionError("报价企业不存在")

    session = _get_session(db, session_id)
    if session.status != OPEN:
        raise AuctionError(f"场次当前为 {session.status}，仅开放场次可报价")
    if price < float(session.reserve_price or 0) - 1e-9:
        raise AuctionError(f"报价不得低于场次保留价 {float(session.reserve_price):.2f} 元/吨")

    account = _get_account(db, company_id, session.year)

    with locked_accounts([auction_session_key(session_id), account_lock_key(account.id)]):
        session = _get_session(db, session_id)
        if session.status != OPEN:
            raise AuctionError("场次已结束报价，提交被拒绝")

        # 幂等命中优先返回（双击/重试），避免把合法重试误判为重复报价
        if idempotency_key:
            same = (
                db.query(AuctionBid)
                .filter(AuctionBid.idempotency_key == idempotency_key)
                .first()
            )
            if same:
                return same

        existing = (
            db.query(AuctionBid)
            .filter(
                AuctionBid.session_id == session_id,
                AuctionBid.company_id == company_id,
                AuctionBid.side == side,
                AuctionBid.status == BID_ACTIVE,
            )
            .first()
        )
        if existing:
            raise AuctionError("本企业在该场次同方向已有有效报价，请先撤单后重新报价")

        qty = round(float(quantity), 4)
        try:
            with transactional(db):
                # 卖出报价原子占用：原子条件 UPDATE 保证
                # current >= frozen + reserved，跨场次/订单并发也不会超卖
                if side == _SELL:
                    account = lock_row_for_write(db, account.id)
                    available = round(
                        float(account.current_balance)
                        - float(account.frozen_balance)
                        - float(account.reserved_balance),
                        4,
                    )
                    if available + 1e-9 < qty:
                        raise InsufficientBalanceError(
                            f"自由可用配额不足：最多可报 {available:g} 吨"
                        )
                    bal, frz, rsv = apply_ledger_delta(db, account.id, 0, 0, qty)

                bid = AuctionBid(
                    bid_no=_gen_bid_no(db),
                    session_id=session_id,
                    company_id=company_id,
                    side=side,
                    year=session.year,
                    quantity=qty,
                    price=round(float(price), 2),
                    tx_date=tx_date or _today(),
                    remark=(remark or "").strip()[:256],
                    created_by=operator.id if operator else None,
                    idempotency_key=idempotency_key,
                )
                db.add(bid)
                db.flush()

                if side == _SELL:
                    _add_ledger_no_trade(
                        db, account, "auction_bid_reserve", qty,
                        bal, frz, rsv, "竞价报价",
                        f"场次 {session.session_no} 卖出报价 {bid.bid_no} 冻结/占用 {qty} 吨 @ {price:.2f}",
                        bid.tx_date,
                    )

                write_audit(
                    db, operator, "bid.place",
                    target_type="bid", target_id=bid.id, session_id=session_id,
                    detail=f"企业 {_company_name(db, company_id)} "
                    f"{'买入' if side == _BUY else '卖出'}报价 {qty:g} 吨 @ {price:.2f}，报价单 {bid.bid_no}",
                )
                db.refresh(bid)
        except InsufficientBalanceError:
            raise AuctionError(
                f"自由可用配额不足（已扣除履约冻结与交易占用），无法报卖 {qty:g} 吨"
            )
        except IntegrityError as exc:
            if idempotency_key and is_duplicate_submit(exc):
                db.rollback()
                same = (
                    db.query(AuctionBid)
                    .filter(AuctionBid.idempotency_key == idempotency_key)
                    .first()
                )
                if same:
                    return same
            # 并发报价撞“同企业同方向有效报价唯一”索引
            if "uq_auction_active_bid" in str(getattr(exc, "orig", exc)) or (
                "UNIQUE constraint failed" in str(getattr(exc, "orig", exc))
                and "auction_bids" in str(getattr(exc, "orig", exc))
            ):
                raise AuctionError("本企业在该场次同方向已有有效报价，请勿重复提交")
            raise
        return bid


def cancel_bid(
    db: Session,
    bid_id: int,
    company_id: int | None,
    operator: Operator | None,
    reason: str = "",
    *,
    as_regulator: bool = False,
) -> AuctionBid:
    """撤销报价。

    - 企业只能撤销本企业报价（company_id 即登录企业）；
    - 监管（as_regulator=True）可撤销任意企业报价；
    - 仅 active 报价可撤；撮合后报价为终态不可撤（须监管撤场）。
    """
    bid = _get_bid(db, bid_id)
    if not as_regulator and company_id != bid.company_id:
        raise AuctionError("无权撤销其他企业的报价")
    if bid.status != BID_ACTIVE:
        raise AuctionError(f"报价当前为 {bid.status}，不可撤销")

    account = _get_account(db, bid.company_id, bid.year)
    with locked_accounts([auction_session_key(bid.session_id), account_lock_key(account.id)]):
        bid = _get_bid(db, bid_id)
        if bid.status == BID_CANCELLED:
            db.refresh(bid)
            return bid
        if bid.status != BID_ACTIVE:
            raise AuctionError("报价已撮合，不可单独撤销；如需终止请由监管撤场")

        try:
            with transactional(db):
                result = db.execute(
                    update(AuctionBid)
                    .where(AuctionBid.id == bid_id, AuctionBid.status == BID_ACTIVE)
                    .values(
                        status=BID_CANCELLED,
                        cancel_reason=(reason or "").strip()[:256],
                        cancelled_by=operator.id if operator else None,
                        cancelled_at=datetime.utcnow(),
                    )
                    .execution_options(synchronize_session=False)
                )
                if result.rowcount != 1:
                    raise AuctionError("报价状态已变化，撤单失败，请刷新后重试")

                # 卖出报价占用的配额当场释放回自由可用（买入报价无占用）
                if bid.side == _SELL:
                    account = lock_row_for_write(db, account.id)
                    qty = _num(bid.quantity)
                    bal, frz, rsv = apply_ledger_delta(db, account.id, 0, 0, -qty)
                    _add_ledger_no_trade(
                        db, account, "auction_bid_release", qty,
                        bal, frz, rsv, "竞价撤单",
                        f"撤销卖出报价 {bid.bid_no}，释放占用 {qty} 吨",
                        bid.tx_date or _today(),
                    )

                write_audit(
                    db, operator, "bid.cancel",
                    target_type="bid", target_id=bid_id, session_id=bid.session_id,
                    detail=("监管" if as_regulator else "企业")
                    + f"撤销报价 {bid.bid_no}（{_company_name(db, bid.company_id)}，"
                    + f"{'买入' if bid.side == _BUY else '卖出'} {_num(bid.quantity):g} 吨）：{reason or '（无原因）'}",
                )
                db.flush()
                db.refresh(bid)
        except InsufficientBalanceError:
            raise AuctionError("释放报价占用失败，账本状态异常，撤单已回滚")
        return bid


# --------------------------------------------------------------------------- #
# 撮合（统一价格双向竞价）
# --------------------------------------------------------------------------- #

def _candidate_prices(buys: list[AuctionBid], sells: list[AuctionBid], reserve_price: float) -> list[float]:
    """候选出清价：所有不低于保留价的买卖报价档位（集合竞价只在报价点上出清）。"""
    prices = {
        round(float(b.price), 2)
        for b in (*buys, *sells)
        if float(b.price) + 1e-9 >= reserve_price
    }
    return sorted(prices)


def _allocate_at_price(
    buy_queue: list[dict],
    sell_queue: list[dict],
) -> list[dict]:
    """在给定价格已筛好的买卖队列上做价格-时间优先配对（不与本企业自成交）。

    买方队列按价格降序、时间升序；卖方队列按价格升序、时间升序。
    每个买方依次在卖方队列中寻找第一家“非本企业且有余额”的卖方成交；
    找不到则把该买方需求量留给后续卖方（继续尝试下一个买方，保证不遗漏
    “当前买方自相关、但后续买方可成交”的供货）。

    返回 ``[{buyer, seller, quantity}, ...]``；同时原地扣减各队列 left。
    """
    pairs: list[dict] = []
    for bq in buy_queue:
        while bq["left"] > 1e-9:
            sq = next(
                (
                    x
                    for x in sell_queue
                    if x["left"] > 1e-9 and x["bid"].company_id != bq["bid"].company_id
                ),
                None,
            )
            if sq is None:
                break
            qty = round(min(bq["left"], sq["left"]), 4)
            pairs.append({"buyer": bq, "seller": sq, "quantity": qty})
            bq["left"] = round(bq["left"] - qty, 4)
            sq["left"] = round(sq["left"] - qty, 4)
    return pairs


def _simulate_volume(
    buys: list[AuctionBid],
    sells: list[AuctionBid],
    price: float,
) -> tuple[float, float, float]:
    """在候选价上模拟配对，返回 (实际可成交量, 买方申报需求, 卖方申报供给)。

    与 :func:`_allocate_at_price` 使用完全相同的配对规则与自成交规避，
    保证选出的出清价一定可以实际执行（不会“有价无量”）。
    """
    bq = [
        {"bid": b, "left": _num(b.quantity)}
        for b in buys
        if float(b.price) + 1e-9 >= price
    ]
    sq = [
        {"bid": s, "left": _num(s.quantity)}
        for s in sells
        if float(s.price) <= price + 1e-9
    ]
    pairs = _allocate_at_price(bq, sq)
    volume = round(sum(p["quantity"] for p in pairs), 4)
    demand = round(sum(_num(b.quantity) for b in buys if float(b.price) + 1e-9 >= price), 4)
    supply = round(sum(_num(s.quantity) for s in sells if float(s.price) <= price + 1e-9), 4)
    return volume, demand, supply


def _determine_clear_price(
    buys: list[AuctionBid],
    sells: list[AuctionBid],
    reserve_price: float,
) -> tuple[float, float] | None:
    """计算统一出清价，返回 ``(出清价, 实际可成交量)``；无可行量返回 None。

    规则：
    1. 最大化实际可成交量（价格-时间优先且规避自成交后的配对量）；
    2. 并列时选未匹配量（|需求-供给|）最小档；
    3. 仍并列取候选档均价（保留两位小数，国内集合竞价惯例）。
    """
    candidates = _candidate_prices(buys, sells, reserve_price)
    best: list[tuple[float, float, float]] = []  # (可成交量, 未匹配量, 价格)
    max_volume = 0.0
    for p in candidates:
        volume, demand, supply = _simulate_volume(buys, sells, p)
        if volume > max_volume + 1e-9:
            max_volume = volume
            best = [(volume, abs(demand - supply), p)]
        elif abs(volume - max_volume) <= 1e-9 and volume > 0:
            best.append((volume, abs(demand - supply), p))
    if not best or max_volume <= 0:
        return None
    min_imbalance = min(x[1] for x in best)
    winners = [x[2] for x in best if abs(x[1] - min_imbalance) <= 1e-9]
    clear_price = round(sum(winners) / len(winners), 2)
    return clear_price, max_volume


def run_matching(db: Session, session_id: int, operator: Operator | None) -> AuctionSession:
    """监管触发统一撮合：定价、价格-时间优先配对、调整卖方占用、生成成交单。

    撮合是单次密封集合竞价：一次撮合后全部报价进入终态（matched/partial/unmatched）。
    卖出报价在提交时已把报量转为交易占用（reserved），撮合阶段只做占用归属调整：
    成交部分保留占用等待结算出库，未成交/未成交余量当场释放回自由可用。
    重复撮合幂等返回。
    """
    with locked_accounts([auction_session_key(session_id)]):
        session = _get_session(db, session_id)
        if session.status == MATCHED:
            db.refresh(session)
            return session
        if session.status == SETTLED:
            raise AuctionError("场次已结算，不能重复撮合")
        if session.status != OPEN:
            raise AuctionError(f"场次当前为 {session.status}，不能撮合")

        bids = (
            db.query(AuctionBid)
            .filter(AuctionBid.session_id == session_id, AuctionBid.status == BID_ACTIVE)
            .all()
        )
        buys = sorted(
            [b for b in bids if b.side == _BUY],
            key=lambda b: (-float(b.price), b.id),
        )
        sells = sorted(
            [b for b in bids if b.side == _SELL],
            key=lambda b: (float(b.price), b.id),
        )

        # 撮合只触碰卖出方账户（释放未成交占用）；账户与场次同事务锁定
        company_ids = {b.company_id for b in sells}
        accounts: dict[int, AllowanceAccount] = {}
        keys: list[str] = []
        for cid in company_ids:
            acc = _get_account(db, cid, session.year)
            accounts[cid] = acc
            keys.append(account_lock_key(acc.id))
            keys.append(company_clear_key(cid, session.year))

        with locked_accounts(sorted(set(keys))):
            try:
                with transactional(db):
                    if _transit_session(db, session_id, (OPEN,), MATCHED) != 1:
                        raise AuctionError("场次状态已变化，撮合失败，请刷新后重试")
                    session = _get_session(db, session_id)
                    reserve_price = float(session.reserve_price or 0)
                    now = datetime.utcnow()
                    tx_date = _today()

                    pricing = _determine_clear_price(buys, sells, reserve_price)

                    if pricing is None:
                        # 无可行成交：释放全部卖出报价占用，全部报价置未成交
                        for sbid in sells:
                            acc = lock_row_for_write(db, accounts[sbid.company_id].id)
                            accounts[sbid.company_id] = acc
                            qty = _num(sbid.quantity)
                            bal, frz, rsv = apply_ledger_delta(db, acc.id, 0, 0, -qty)
                            _add_ledger_no_trade(
                                db, acc, "auction_bid_release", qty, bal, frz, rsv,
                                "竞价撤单", f"场次 {session.session_no} 无成交，释放卖出报价 {sbid.bid_no} 占用 {qty} 吨",
                                tx_date,
                            )
                        db.execute(
                            update(AuctionBid)
                            .where(
                                AuctionBid.session_id == session_id,
                                AuctionBid.status == BID_ACTIVE,
                            )
                            .values(status=BID_UNMATCHED, matched_at=now)
                            .execution_options(synchronize_session=False)
                        )
                        session = _get_session(db, session_id)
                        session.matched_at = now
                        session.clear_price = None
                        session.matched_volume = 0
                        write_audit(
                            db, operator, "session.match",
                            target_type="session", target_id=session_id, session_id=session_id,
                            detail=f"场次 {session.session_no} 撮合完成：无可成交报价（{len(buys)} 买 / {len(sells)} 卖）",
                        )
                        db.flush()
                        db.refresh(session)
                        return session

                    clear_price, _ = pricing

                    # 出清价下的有效队列（实际配对器，与定价模拟完全一致）
                    buy_queue = [
                        {"bid": b, "left": _num(b.quantity)}
                        for b in buys
                        if float(b.price) + 1e-9 >= clear_price
                    ]
                    sell_queue = [
                        {"bid": s, "left": _num(s.quantity)}
                        for s in sells
                        if float(s.price) <= clear_price + 1e-9
                    ]
                    pairs = _allocate_at_price(buy_queue, sell_queue)

                    trades: list[AuctionTrade] = []
                    filled_by_bid: dict[int, float] = {}
                    sell_unreleased: dict[int, float] = {}  # 卖方已成交、保留占用的数量
                    for seq, pair in enumerate(pairs, start=1):
                        bq, sq, qty = pair["buyer"], pair["seller"], pair["quantity"]
                        trade = AuctionTrade(
                            trade_no=_gen_trade_no(db),
                            session_id=session_id,
                            buyer_bid_id=bq["bid"].id,
                            seller_bid_id=sq["bid"].id,
                            buyer_id=bq["bid"].company_id,
                            seller_id=sq["bid"].company_id,
                            year=session.year,
                            quantity=qty,
                            price=clear_price,
                            alloc_seq=seq,
                            status=TRADE_RESERVED,
                        )
                        db.add(trade)
                        db.flush()
                        filled_by_bid[bq["bid"].id] = round(filled_by_bid.get(bq["bid"].id, 0.0) + qty, 4)
                        filled_by_bid[sq["bid"].id] = round(filled_by_bid.get(sq["bid"].id, 0.0) + qty, 4)
                        sell_unreleased[sq["bid"].id] = round(
                            sell_unreleased.get(sq["bid"].id, 0.0) + qty, 4
                        )
                        trades.append(trade)

                    # 卖方占用归属调整：未成交/部分成交的报价余量当场释放回自由可用
                    for sbid in sells:
                        filled = round(filled_by_bid.get(sbid.id, 0.0), 4)
                        leftover = round(_num(sbid.quantity) - filled, 4)
                        if leftover > 1e-9:
                            acc = lock_row_for_write(db, accounts[sbid.company_id].id)
                            accounts[sbid.company_id] = acc
                            bal, frz, rsv = apply_ledger_delta(db, acc.id, 0, 0, -leftover)
                            _add_ledger_no_trade(
                                db, acc, "auction_bid_release", leftover, bal, frz, rsv,
                                "竞价撮合",
                                f"场次 {session.session_no} 撮合，卖出报价 {sbid.bid_no} 未成交余量 "
                                f"{leftover} 吨释放",
                                tx_date,
                            )

                    # 回写报价成交状态
                    for b in (*buys, *sells):
                        filled = round(filled_by_bid.get(b.id, 0.0), 4)
                        if filled <= 0:
                            b.status = BID_UNMATCHED
                        elif filled + 1e-9 >= _num(b.quantity):
                            b.status = BID_MATCHED
                            b.filled_quantity = _num(b.quantity)
                        else:
                            b.status = BID_PARTIAL
                            b.filled_quantity = filled
                        b.matched_at = now

                    total_volume = round(sum(_num(t.quantity) for t in trades), 4)
                    session = _get_session(db, session_id)
                    session.clear_price = clear_price
                    session.matched_volume = total_volume
                    session.trade_count = len(trades)
                    session.matched_at = now
                    write_audit(
                        db, operator, "session.match",
                        target_type="session", target_id=session_id, session_id=session_id,
                        detail=f"场次 {session.session_no} 撮合完成：出清价 {clear_price:.2f} 元/吨，"
                        f"成交 {len(trades)} 笔 / {total_volume:g} 吨",
                    )
                    db.flush()
                    db.refresh(session)
            except InsufficientBalanceError:
                raise AuctionError("撮合调整卖方占用失败，撮合已整体回滚")
            return session


# --------------------------------------------------------------------------- #
# 结算（并发安全：状态抢占，只有一个事务成功）
# --------------------------------------------------------------------------- #

def settle_session(db: Session, session_id: int, operator: Operator | None) -> AuctionSession:
    """监管触发场次统一结算：卖方占用出库、买方到账、买方缺口核销同一事务完成。

    已结算场次重复调用幂等返回；撮合/撤场与结算并发时由场次状态条件 UPDATE 抢占。
    """
    with locked_accounts([auction_session_key(session_id)]):
        session = _get_session(db, session_id)
        if session.status == SETTLED:
            db.refresh(session)
            return session
        if session.status == CANCELLED:
            raise AuctionError("场次已撤销，不能结算")
        if session.status != MATCHED:
            raise AuctionError(f"场次当前为 {session.status}，尚未撮合，不能结算")

        trades = (
            db.query(AuctionTrade)
            .filter(AuctionTrade.session_id == session_id)
            .order_by(AuctionTrade.alloc_seq.asc(), AuctionTrade.id.asc())
            .all()
        )
        reserved_trades = [t for t in trades if t.status == TRADE_RESERVED]

        # 收集全部买卖双方账户键与清缴键，按序加锁
        company_ids = {c for t in reserved_trades for c in (t.seller_id, t.buyer_id)}
        accounts: dict[int, AllowanceAccount] = {}
        keys: list[str] = []
        for cid in company_ids:
            acc = _get_account(db, cid, session.year)
            accounts[cid] = acc
            keys.append(account_lock_key(acc.id))
            keys.append(company_clear_key(cid, session.year))

        with locked_accounts(sorted(set(keys))):
            try:
                with transactional(db):
                    if _transit_session(db, session_id, (MATCHED,), SETTLED) != 1:
                        raise AuctionError("场次状态已变化，结算失败，请刷新后重试")
                    session = _get_session(db, session_id)
                    tx_date = _today()
                    auto_clear = bool(int(session.auto_clear_deficit or 0))

                    buyer_trades: dict[int, list[AuctionTrade]] = {}
                    for trade in reserved_trades:
                        seller_acc = lock_row_for_write(db, accounts[trade.seller_id].id)
                        accounts[trade.seller_id] = seller_acc
                        buyer_acc = lock_row_for_write(db, accounts[trade.buyer_id].id)
                        accounts[trade.buyer_id] = buyer_acc
                        qty = _round4(trade.quantity)

                        buyer_name = _company_name(db, trade.buyer_id)
                        seller_name = _company_name(db, trade.seller_id)

                        # 卖方：撮合占用配额正式出库（current/reserved 同减，frozen 不变）
                        s_bal, s_frz, s_rsv = apply_ledger_delta(
                            db, seller_acc.id, -qty, 0, -qty
                        )
                        _add_ledger(
                            db, seller_acc, "auction_deliver_out", qty,
                            s_bal, s_frz, s_rsv, buyer_name,
                            f"场次 {session.session_no} 结算划出 {qty} 吨 @ {float(trade.price):.2f}，"
                            f"成交单 {trade.trade_no}",
                            trade, tx_date,
                        )

                        # 买方：配额到账（不动既有冻结/占用）
                        b_bal, b_frz, b_rsv = apply_ledger_delta(db, buyer_acc.id, qty, 0, 0)
                        _add_ledger(
                            db, buyer_acc, "auction_deliver_in", qty,
                            b_bal, b_frz, b_rsv, seller_name,
                            f"场次 {session.session_no} 结算受让 {qty} 吨 @ {float(trade.price):.2f}，"
                            f"成交单 {trade.trade_no}",
                            trade, tx_date,
                        )

                        trade.status = TRADE_SETTLED
                        trade.settled_at = datetime.utcnow()
                        buyer_trades.setdefault(trade.buyer_id, []).append(trade)

                    # 买方年度配额闭环：该买方全部成交单到账后统一核销一次缺口
                    # （先核销冻结配额，再用刚到账的自由可用配额补缴，绝不触碰占用）
                    clearance_count = 0
                    for buyer_id, its in buyer_trades.items():
                        record = settle_buyer_deficit_on_auction(
                            db, its[-1], tx_date, auto_clear=auto_clear
                        )
                        if record is not None:
                            clearance_count += 1

                    session = _get_session(db, session_id)
                    session.settled_at = datetime.utcnow()
                    write_audit(
                        db, operator, "session.settle",
                        target_type="session", target_id=session_id, session_id=session_id,
                        detail=f"场次 {session.session_no} 统一结算：成交 {len(reserved_trades)} 笔 / "
                        f"{_num(session.matched_volume):g} 吨全部划转，联动核销买方缺口 {clearance_count} 家",
                    )
                    db.flush()
                    db.refresh(session)
            except InsufficientBalanceError:
                raise AuctionError("结算划转失败，账本状态异常，结算已整体回滚")
            except ValueError as exc:
                if isinstance(exc, AuctionError):
                    raise
                raise AuctionError(f"结算联动履约核销失败，整笔结算已回滚：{exc}")
            return session


# --------------------------------------------------------------------------- #
# 查询辅助
# --------------------------------------------------------------------------- #

def list_sessions(db: Session, *, year: int | None = None, status: str | None = None) -> list[AuctionSession]:
    q = db.query(AuctionSession)
    if year is not None:
        q = q.filter(AuctionSession.year == year)
    if status:
        q = q.filter(AuctionSession.status == status)
    return q.order_by(AuctionSession.id.desc()).all()


def list_bids(
    db: Session,
    *,
    session_id: int | None = None,
    company_id: int | None = None,
    status: str | None = None,
) -> list[AuctionBid]:
    q = db.query(AuctionBid)
    if session_id is not None:
        q = q.filter(AuctionBid.session_id == session_id)
    if company_id is not None:
        q = q.filter(AuctionBid.company_id == company_id)
    if status:
        q = q.filter(AuctionBid.status == status)
    return q.order_by(AuctionBid.id.asc()).all()


def list_trades(
    db: Session,
    *,
    session_id: int | None = None,
    company_id: int | None = None,
) -> list[AuctionTrade]:
    q = db.query(AuctionTrade)
    if session_id is not None:
        q = q.filter(AuctionTrade.session_id == session_id)
    if company_id is not None:
        q = q.filter(
            (AuctionTrade.buyer_id == company_id) | (AuctionTrade.seller_id == company_id)
        )
    return q.order_by(AuctionTrade.alloc_seq.asc(), AuctionTrade.id.asc()).all()


def list_audit_logs(
    db: Session,
    *,
    session_id: int | None = None,
    limit: int = 200,
) -> list[AuctionAuditLog]:
    q = db.query(AuctionAuditLog)
    if session_id is not None:
        q = q.filter(AuctionAuditLog.session_id == session_id)
    return q.order_by(AuctionAuditLog.id.desc()).limit(limit).all()
