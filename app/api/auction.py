"""碳配额集中竞价市场 API：监管（场次/撮合/结算/审计）与买/卖方（报价/撤单/成交查询）。

权限边界：
- 场次管理（建场/开放/撮合/结算/撤场）仅 admin；verifier 只读；
- 企业仅可为本企业报价/撤单，仅可查看本企业参与的成交；
- 审计日志仅 admin/verifier 可见；一切敏感操作与越权拒绝均写审计。
"""

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user, require_roles
from app.models import (
    AuctionBid,
    AuctionSession,
    AuctionTrade,
    Company,
    User,
)
from app.schemas import (
    AuctionBidCancelIn,
    AuctionBidIn,
    AuctionSessionCancelIn,
    AuctionSessionIn,
    AuctionTradeDefaultIn,
    AuctionTradeReverseIn,
)
from app.services.auction_service import (
    AuctionError,
    Operator,
    REVERSAL_BUYER_DEFAULT,
    REVERSAL_SELLER_DEFAULT,
    REVERSAL_REGULATOR,
    cancel_bid,
    cancel_session,
    create_session,
    list_audit_logs,
    list_bids,
    list_sessions,
    list_trade_reversals,
    list_trades,
    open_session,
    place_bid,
    reverse_settled_trade,
    run_matching,
    settle_session,
    write_audit,
)

router = APIRouter(prefix="/api/auctions", tags=["auctions"])


def _operator(user: User, request: Request) -> Operator:
    return Operator(
        id=user.id,
        username=user.username,
        role=user.role,
        ip=(request.client.host if request.client else "") or "",
    )


def _audit_denied(db: Session, user: User, request: Request, action: str, detail: str) -> None:
    """越权拒绝即时落审计（无业务事务，独立提交）。"""
    write_audit(
        db,
        _operator(user, request),
        action,
        detail=detail,
        result="denied",
        commit=True,
    )


def _serialize_session(db: Session, s: AuctionSession) -> dict:
    creator = db.get(User, s.created_by) if s.created_by else None
    bids = list_bids(db, session_id=s.id)
    active_bids = [b for b in bids if b.status == "active"]
    return {
        "id": s.id,
        "session_no": s.session_no,
        "name": s.name,
        "year": s.year,
        "product": s.product,
        "reserve_price": float(s.reserve_price or 0),
        "estimated_volume": float(s.estimated_volume) if s.estimated_volume is not None else None,
        "status": s.status,
        "clear_price": float(s.clear_price) if s.clear_price is not None else None,
        "matched_volume": float(s.matched_volume or 0),
        "trade_count": s.trade_count or 0,
        "bid_count": len(active_bids),
        "auto_clear_deficit": bool(s.auto_clear_deficit),
        "open_at": s.open_at,
        "close_at": s.close_at,
        "matched_at": s.matched_at,
        "settled_at": s.settled_at,
        "cancelled_at": s.cancelled_at,
        "cancel_reason": s.cancel_reason,
        "created_by": s.created_by,
        "created_by_name": creator.display_name if creator else (creator.username if creator else ""),
        "remark": s.remark,
        "created_at": s.created_at,
    }


def _serialize_bid(db: Session, b: AuctionBid) -> dict:
    company = db.get(Company, b.company_id)
    return {
        "id": b.id,
        "bid_no": b.bid_no,
        "session_id": b.session_id,
        "company_id": b.company_id,
        "company_name": company.name if company else str(b.company_id),
        "side": b.side,
        "year": b.year,
        "quantity": float(b.quantity),
        "price": float(b.price),
        "filled_quantity": float(b.filled_quantity or 0),
        "status": b.status,
        "tx_date": b.tx_date,
        "remark": b.remark,
        "cancel_reason": b.cancel_reason,
        "matched_at": b.matched_at,
        "cancelled_at": b.cancelled_at,
        "created_at": b.created_at,
    }


def _serialize_trade(db: Session, t: AuctionTrade) -> dict:
    buyer = db.get(Company, t.buyer_id)
    seller = db.get(Company, t.seller_id)
    return {
        "id": t.id,
        "trade_no": t.trade_no,
        "session_id": t.session_id,
        "buyer_id": t.buyer_id,
        "seller_id": t.seller_id,
        "buyer_name": buyer.name if buyer else str(t.buyer_id),
        "seller_name": seller.name if seller else str(t.seller_id),
        "year": t.year,
        "quantity": float(t.quantity),
        "price": float(t.price),
        "alloc_seq": t.alloc_seq,
        "status": t.status,
        "reversed_quantity": float(t.reversed_quantity or 0),
        "cleared_quantity": float(t.cleared_quantity or 0),
        "cleared_refunded_quantity": float(t.cleared_refunded_quantity or 0),
        "settled_at": t.settled_at,
        "last_reversed_at": t.last_reversed_at,
        "cancelled_at": t.cancelled_at,
        "created_at": t.created_at,
    }


def _serialize_reversal(db: Session, r) -> dict:
    buyer = db.get(Company, r.buyer_id)
    seller = db.get(Company, r.seller_id)
    trade = db.get(AuctionTrade, r.trade_id)
    return {
        "id": r.id,
        "reversal_no": r.reversal_no,
        "trade_id": r.trade_id,
        "trade_no": trade.trade_no if trade else str(r.trade_id),
        "session_id": r.session_id,
        "buyer_id": r.buyer_id,
        "seller_id": r.seller_id,
        "buyer_name": buyer.name if buyer else str(r.buyer_id),
        "seller_name": seller.name if seller else str(r.seller_id),
        "year": r.year,
        "kind": r.kind,
        "request_quantity": float(r.request_quantity),
        "reversed_quantity": float(r.reversed_quantity),
        "cleared_refund_quantity": float(r.cleared_refund_quantity),
        "defaulted_quantity": float(r.defaulted_quantity),
        "reason": r.reason,
        "status": r.status,
        "operator_name": r.operator_name,
        "tx_date": r.tx_date,
        "created_at": r.created_at,
    }


# --------------------------------------------------------------------------- #
# 场次
# --------------------------------------------------------------------------- #

@router.get("")
def get_sessions(
    year: int | None = None,
    status: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    return [_serialize_session(db, s) for s in list_sessions(db, year=year, status=status)]


@router.get("/my-trades")
def my_trades(
    session_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """企业仅见本企业参与的成交；监管/核查角色可见全部。"""
    company_id = user.company_id if user.role == "enterprise" else None
    trades = list_trades(db, session_id=session_id, company_id=company_id)
    return [_serialize_trade(db, t) for t in trades]


@router.get("/audit-logs")
def audit_logs(
    request: Request,
    session_id: int | None = None,
    limit: int = 200,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    if user.role not in ("admin", "verifier"):
        _audit_denied(
            db, user, request, "audit.read",
            f"企业 {user.company_id} 试图读取竞价审计日志",
        )
        raise HTTPException(status_code=403, detail="仅监管角色可查看审计日志")
    limit = max(1, min(limit, 500))
    logs = list_audit_logs(db, session_id=session_id, limit=limit)
    return [
        {
            "id": x.id,
            "operator_id": x.operator_id,
            "operator_name": x.operator_name,
            "operator_role": x.operator_role,
            "action": x.action,
            "target_type": x.target_type,
            "target_id": x.target_id,
            "session_id": x.session_id,
            "detail": x.detail,
            "result": x.result,
            "ip": x.ip,
            "created_at": x.created_at,
        }
        for x in logs
    ]


@router.post("")
def create(
    request: Request,
    data: AuctionSessionIn,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin")),
):
    idem = data.idempotency_key or request.headers.get("idempotency-key")
    try:
        session = create_session(
            db,
            year=data.year,
            name=data.name,
            reserve_price=data.reserve_price,
            estimated_volume=data.estimated_volume,
            product=data.product,
            auto_clear_deficit=data.auto_clear_deficit,
            remark=data.remark,
            open_at=data.open_at,
            close_at=data.close_at,
            operator=_operator(user, request),
            idempotency_key=idem,
        )
    except AuctionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _serialize_session(db, session)


@router.get("/{session_id}")
def detail(session_id: int, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    session = db.get(AuctionSession, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="竞价场次不存在")
    return _serialize_session(db, session)


@router.post("/{session_id}/open")
def do_open(session_id: int, request: Request, db: Session = Depends(get_db),
            user: User = Depends(require_roles("admin"))):
    try:
        session = open_session(db, session_id, _operator(user, request))
    except AuctionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _serialize_session(db, session)


@router.post("/{session_id}/match")
def do_match(session_id: int, request: Request, db: Session = Depends(get_db),
             user: User = Depends(require_roles("admin"))):
    try:
        session = run_matching(db, session_id, _operator(user, request))
    except AuctionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _serialize_session(db, session)


@router.post("/{session_id}/settle")
def do_settle(session_id: int, request: Request, db: Session = Depends(get_db),
              user: User = Depends(require_roles("admin"))):
    try:
        session = settle_session(db, session_id, _operator(user, request))
    except AuctionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _serialize_session(db, session)


@router.post("/{session_id}/cancel")
def do_cancel(
    session_id: int,
    data: AuctionSessionCancelIn,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin")),
):
    try:
        session = cancel_session(db, session_id, _operator(user, request), data.reason)
    except AuctionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _serialize_session(db, session)


# --------------------------------------------------------------------------- #
# 报价
# --------------------------------------------------------------------------- #

@router.get("/{session_id}/bids")
def get_bids(
    session_id: int,
    status_filter: str | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    if not db.get(AuctionSession, session_id):
        raise HTTPException(status_code=404, detail="竞价场次不存在")
    company_id = user.company_id if user.role == "enterprise" else None
    bids = list_bids(db, session_id=session_id, company_id=company_id, status=status_filter)
    return [_serialize_bid(db, b) for b in bids]


@router.post("/{session_id}/bids")
def place(
    session_id: int,
    data: AuctionBidIn,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "enterprise")),
):
    # 企业只能为本企业报价；监管代企业报价不从此入口开放（企业自主密封报价）
    if user.role == "enterprise":
        company_id = user.company_id
    else:
        raise HTTPException(status_code=403, detail="监管账号不参与报价，请使用企业账号")
    idem = data.idempotency_key or request.headers.get("idempotency-key")
    try:
        bid = place_bid(
            db,
            session_id,
            company_id,
            data.side,
            data.quantity,
            data.price,
            tx_date=data.tx_date,
            remark=data.remark,
            operator=_operator(user, request),
            idempotency_key=idem,
        )
    except AuctionError as e:
        # 业务拒绝（余额不足/重复报价/非开放状态）也留痕，便于监管审计异常报价行为
        write_audit(
            db, _operator(user, request), "bid.place",
            target_type="session", session_id=session_id,
            detail=f"报价被拒绝（{data.side} {data.quantity} 吨 @ {data.price}）：{e}",
            result="denied", commit=True,
        )
        raise HTTPException(status_code=400, detail=str(e))
    return _serialize_bid(db, bid)


@router.post("/bids/{bid_id}/cancel")
def bid_cancel(
    bid_id: int,
    data: AuctionBidCancelIn,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "enterprise")),
):
    bid = db.get(AuctionBid, bid_id)
    if not bid:
        raise HTTPException(status_code=404, detail="报价单不存在")
    as_regulator = user.role == "admin"
    if not as_regulator and user.company_id != bid.company_id:
        _audit_denied(
            db, user, request, "bid.cancel",
            f"企业 {user.company_id} 试图撤销企业 {bid.company_id} 的报价 {bid.bid_no}",
        )
        raise HTTPException(status_code=403, detail="无权撤销其他企业的报价")
    try:
        bid = cancel_bid(
            db, bid_id,
            user.company_id if not as_regulator else None,
            _operator(user, request),
            data.reason,
            as_regulator=as_regulator,
        )
    except AuctionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _serialize_bid(db, bid)


# --------------------------------------------------------------------------- #
# 成交
# --------------------------------------------------------------------------- #

@router.get("/trades/all")
def all_trades(
    request: Request,
    session_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """监管/核查侧查看全部成交；企业请用 /api/auctions/my-trades。"""
    if user.role == "enterprise":
        _audit_denied(
            db, user, request, "trade.read",
            f"企业 {user.company_id} 试图读取全市场成交明细",
        )
        raise HTTPException(status_code=403, detail="企业仅可查看本企业参与的成交：/api/auctions/my-trades")
    trades = list_trades(db, session_id=session_id)
    return [_serialize_trade(db, t) for t in trades]


# --------------------------------------------------------------------------- #
# 已结算成交单：监管冲正 / 违约回退
# --------------------------------------------------------------------------- #

def _do_reverse(
    request: Request,
    trade_id: int,
    db: Session,
    user: User,
    *,
    kind: str,
    reason: str,
    quantity: float | None,
    allow_partial: bool,
    tx_date: str,
    idem: str | None,
    action: str,
):
    trade = db.get(AuctionTrade, trade_id)
    if not trade:
        raise HTTPException(status_code=404, detail="成交单不存在")
    try:
        reversal = reverse_settled_trade(
            db, trade_id,
            kind=kind,
            reason=reason,
            quantity=quantity,
            allow_partial=allow_partial,
            tx_date=tx_date,
            operator=_operator(user, request),
            idempotency_key=idem,
        )
    except AuctionError as e:
        # 监管冲正/违约回退被拒（状态非法、足额不足等）也留痕
        write_audit(
            db, _operator(user, request), action,
            target_type="trade", target_id=trade_id, session_id=trade.session_id,
            detail=f"{action} 被拒绝（成交单 {trade.trade_no}）：{e}",
            result="denied", commit=True,
        )
        raise HTTPException(status_code=400, detail=str(e))
    return _serialize_reversal(db, reversal)


@router.post("/trades/{trade_id}/reverse")
def trade_reverse(
    trade_id: int,
    data: AuctionTradeReverseIn,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin")),
):
    """监管冲正已结算成交：配额/流水/履约清缴/审计同事务回退，支持部分冲正。"""
    idem = data.idempotency_key or request.headers.get("idempotency-key")
    return _do_reverse(
        request, trade_id, db, user,
        kind=REVERSAL_REGULATOR,
        reason=data.reason,
        quantity=data.quantity,
        allow_partial=data.allow_partial,
        tx_date=data.tx_date,
        idem=idem,
        action="trade.reverse",
    )


@router.post("/trades/{trade_id}/default")
def trade_default(
    trade_id: int,
    data: AuctionTradeDefaultIn,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin")),
):
    """违约回退已结算成交：从违约方对手侧尽力追回，欠量挂账（允许部分回退）。"""
    idem = data.idempotency_key or request.headers.get("idempotency-key")
    kind = REVERSAL_BUYER_DEFAULT if data.side == "buyer" else REVERSAL_SELLER_DEFAULT
    return _do_reverse(
        request, trade_id, db, user,
        kind=kind,
        reason=data.reason,
        quantity=data.quantity,
        allow_partial=True,
        tx_date=data.tx_date,
        idem=idem,
        action="trade.default",
    )


@router.get("/trades/{trade_id}/reversals")
def trade_reversals(
    request: Request,
    trade_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """成交单回退记录：监管/核查可查任意单；企业仅限本企业参与的成交。"""
    trade = db.get(AuctionTrade, trade_id)
    if not trade:
        raise HTTPException(status_code=404, detail="成交单不存在")
    if user.role == "enterprise" and user.company_id not in (trade.buyer_id, trade.seller_id):
        _audit_denied(
            db, user, request, "trade.read",
            f"企业 {user.company_id} 试图读取成交 {trade.trade_no} 的冲正/回退记录",
        )
        raise HTTPException(status_code=403, detail="仅成交参与方或监管可查看回退记录")
    rows = list_trade_reversals(db, trade_id=trade_id)
    return [_serialize_reversal(db, r) for r in rows]


@router.get("/reversals/all")
def all_reversals(
    request: Request,
    session_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """全部冲正/违约回退单（监管/核查）；企业 403 并审计。"""
    if user.role == "enterprise":
        _audit_denied(
            db, user, request, "trade.read",
            f"企业 {user.company_id} 试图读取全市场冲正/回退记录",
        )
        raise HTTPException(status_code=403, detail="企业仅可查看本企业成交的回退记录")
    rows = list_trade_reversals(db, session_id=session_id)
    return [_serialize_reversal(db, r) for r in rows]
