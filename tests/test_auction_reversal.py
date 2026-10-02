"""已结算竞价成交的监管冲正与违约回退链路测试（服务层）。

覆盖：
- 监管冲正：清缴退还（auction_deficit_clear 归因量）、买方追回、卖方退还、
  履约缺口回补与配额状态重算、流水五快照链守恒；
- 部分冲正：按比例退还清缴归因、累计回退封顶、终态/中间态流转、比例残差兜底；
- 违约回退：买方自由可用不足时尽力追回，欠量挂账（defaulted_quantity），
  成交单置 defaulted；监管严格冲正在不足时拒绝且整体回滚；
- 无清缴联动（auto_clear=False / 买方无履约记录）成交的冲正；
- 幂等与并发：相同幂等键只生成一张回退单；多线程并发冲正累计不超过成交量、
  不重复追回/退还，回退与手动清缴并发后配额守恒、流水链一致；
- 审计：冲正/违约动作与拒绝均写审计。
"""

from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from app.core.database import Base
from app.core.ledger import apply_ledger_delta, lock_row_for_write
from app.models import (
    AllowanceAccount,
    AllowanceTransaction,
    AuctionTrade,
    AuctionTradeReversal,
    Company,
    ComplianceRecord,
)
from app.services.auction_service import (
    REVERSAL_BUYER_DEFAULT,
    REVERSAL_REGULATOR,
    REVERSAL_SELLER_DEFAULT,
    TRADE_DEFAULTED,
    TRADE_PARTIAL_REVERSED,
    TRADE_REVERSED,
    TRADE_SETTLED,
    AuctionError,
    Operator,
    create_session,
    list_audit_logs,
    list_trade_reversals,
    open_session,
    place_bid,
    reverse_settled_trade,
    run_matching,
    settle_session,
)
from app.services.quota_service import allocate_quota

YEAR = 2026
ADMIN = Operator(id=1, username="admin", role="admin")


def approx(value, rel=1e-6):
    return pytest.approx(float(value), rel=rel)


@pytest.fixture()
def db(tmp_path):
    """文件型临时库：多线程共享同一份数据。"""
    engine = create_engine(
        f"sqlite:///{tmp_path / 'auction_reversal.db'}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )

    @event.listens_for(engine, "connect")
    def _busy_timeout(dbapi_conn, _rec):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA busy_timeout=30000")
        cur.close()

    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, autoflush=False)()
    yield session
    session.close()
    engine.dispose()


def _fresh(db):
    return Session(bind=db.bind)


def _account(db, company_id, year=YEAR):
    return db.query(AllowanceAccount).filter_by(company_id=company_id, year=year).one()


def _make_company(db, code, name, quota):
    c = Company(code=code, name=name, industry="电力", region="华东")
    db.add(c)
    db.flush()
    allocate_quota(db, c.id, YEAR, baseline=quota, allocation_amount=quota)
    return c


def _make_buyer_deficit(db, company, emission, factor=0.5703):
    """构造买方年度缺口：活动数据→核算→报告批准（冻结+缺口）。"""
    from app.models import ActivityData, CalculationMethod, EmissionFactor, EmissionScope
    from app.services.calculation_service import recalc_company_year
    from app.services.mrv_service import approve_report, generate_report, submit_report

    db.add(EmissionScope(company_id=company.id, scope="2", category="外购电力", name="厂区用电"))
    db.add(CalculationMethod(
        method_code="ELEC", name="外购电力排放因子法", scope="2", formula_type="activity_factor"))
    db.add(EmissionFactor(
        factor_code="ELEC-GRID", name="外购电力", scope="2", unit="tCO2/MWh", value=factor,
        source="电网因子", valid_from="2025-01-01", valid_to="2026-12-31"))
    db.flush()
    scope_id = db.query(EmissionScope).filter_by(company_id=company.id, scope="2").one().id
    db.add(ActivityData(
        company_id=company.id, scope_id=scope_id, year=YEAR, period="monthly",
        activity_type="外购电力", unit="MWh",
        quantity=round(emission / factor, 6), data_source="台账", verified=1))
    db.commit()
    recalc_company_year(db, company.id, YEAR)
    report = generate_report(db, company.id, YEAR)
    submit_report(db, report)
    approve_report(db, report, verifier_id=1)
    db.commit()


def _settled_market(db, *, emission=600, qty=400, seller_quota=1000, buyer_quota=200,
                    auto_clear=True):
    """搭建一场已结算竞价：卖方 S1 出让 qty，买方 B1 受让。"""
    seller = _make_company(db, "S1", "卖方甲", seller_quota)
    buyer = _make_company(db, "B1", "买方丙", buyer_quota)
    db.commit()
    if emission:
        _make_buyer_deficit(db, buyer, emission)
    s = create_session(
        db, year=YEAR, name="回退测试场", auto_clear_deficit=auto_clear, operator=ADMIN)
    open_session(db, s.id, ADMIN)
    place_bid(db, s.id, seller.id, "sell", qty, 80, operator=ADMIN)
    place_bid(db, s.id, buyer.id, "buy", qty, 90, operator=ADMIN)
    run_matching(db, s.id, ADMIN)
    settle_session(db, s.id, ADMIN)
    # 显式结束事务并关闭可能残留的读快照：服务内部 commit 只结束其事务，
    # fixture 共享连接上若残留 SELECT 开启的快照事务，会读不到 settled 终态
    db.commit()
    db.expire_all()
    trade = db.query(AuctionTrade).filter_by(session_id=s.id).one()
    db.commit()
    return seller, buyer, trade


def _assert_snapshot(db, account_id):
    """流水快照链可推算，末笔快照等于账户现值（含冲正/回退三类新流水）。"""
    txs = (
        db.query(AllowanceTransaction)
        .filter(AllowanceTransaction.account_id == account_id)
        .order_by(AllowanceTransaction.id.asc())
        .all()
    )
    current_pos = {
        "allocation": 1, "buy": 1, "transfer_in": 1, "reversal": 1,
        "trade_deliver_in": 1, "auction_deliver_in": 1,
        "auction_clearance_reversal": 1, "auction_reversal_return": 1,
        "sell": -1, "transfer_out": -1, "offset": -1, "clear": -1,
        "frozen_clear": -1, "trade_deficit_clear": -1, "auction_deficit_clear": -1,
        "trade_deliver_out": -1, "auction_deliver_out": -1,
        "auction_reversal_clawback": -1,
        "freeze": 0, "trade_reserve": 0, "trade_release": 0,
        "auction_reserve": 0, "auction_reserve_release": 0,
        "auction_bid_reserve": 0, "auction_bid_release": 0,
    }
    frozen_pos = {"freeze": 1, "frozen_clear": -1, "reversal": -1}
    reserved_pos = {
        "trade_reserve": 1, "trade_release": -1, "trade_deliver_out": -1,
        "auction_reserve": 1, "auction_reserve_release": -1, "auction_deliver_out": -1,
        "auction_bid_reserve": 1, "auction_bid_release": -1,
    }
    ec = ef = er = 0.0
    for t in txs:
        ec = round(ec + current_pos.get(t.tx_type, 0) * float(t.amount), 4)
        ef = round(ef + frozen_pos.get(t.tx_type, 0) * float(t.amount), 4)
        er = round(er + reserved_pos.get(t.tx_type, 0) * float(t.amount), 4)
        assert float(t.balance_after) == approx(ec), f"流水#{t.id} 持仓快照不符（{t.tx_type}）"
        assert float(t.frozen_after or 0) == approx(ef), f"流水#{t.id} 冻结快照不符（{t.tx_type}）"
        assert float(t.reserved_after or 0) == approx(er), f"流水#{t.id} 占用快照不符（{t.tx_type}）"
    acc = db.get(AllowanceAccount, account_id)
    assert float(acc.current_balance) == approx(ec)
    assert float(acc.frozen_balance) == approx(ef)
    assert float(acc.reserved_balance) == approx(er)
    assert float(acc.frozen_balance) + float(acc.reserved_balance) <= float(acc.current_balance) + 1e-9


class TestRegulatorReversal:
    def test_full_reversal_restores_pre_settle_state(self, db):
        """缺口 400 全额冲正：买卖双方配额、履约记录、流水链全部回到结算前。"""
        seller, buyer, trade = _settled_market(db, emission=600, qty=400)

        rv = reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="监管核查异常冲正")
        db.expire_all()
        trade = db.get(AuctionTrade, trade.id)

        assert trade.status == TRADE_REVERSED
        assert float(trade.reversed_quantity) == approx(400)
        assert float(trade.cleared_quantity) == approx(400)
        assert float(trade.cleared_refunded_quantity) == approx(400)
        assert rv.status == "completed"
        assert float(rv.reversed_quantity) == approx(400)
        assert float(rv.cleared_refund_quantity) == approx(400)
        assert float(rv.defaulted_quantity) == 0

        s_acc = _account(db, seller.id)
        b_acc = _account(db, buyer.id)
        assert float(s_acc.current_balance) == approx(1000)  # 出库 400 已退还
        # 买方 200 冻结已通过 frozen_clear 清缴离开账户（不随竞价冲正回退），
        # 到账 400 先退还清缴再追回，自身持仓最终为 0；履约缺口回到 400
        assert float(b_acc.current_balance) == 0
        assert float(b_acc.frozen_balance) == 0

        record = db.query(ComplianceRecord).filter_by(
            company_id=buyer.id, is_active=1).one()
        # 冻结清缴 200 不随竞价冲正回退；到账补缴 400 退还，缺口回到 400
        assert record.status == "deficit"
        assert float(record.cleared_amount) == approx(200)
        assert float(record.deficit) == approx(400)

        assert db.query(AllowanceTransaction).filter_by(
            tx_type="auction_clearance_reversal").count() == 1
        assert db.query(AllowanceTransaction).filter_by(
            tx_type="auction_reversal_clawback").count() == 1
        ret = db.query(AllowanceTransaction).filter_by(
            tx_type="auction_reversal_return").one()
        assert float(ret.amount) == approx(400)
        assert ret.auction_trade_id == trade.id

        _assert_snapshot(db, s_acc.id)
        _assert_snapshot(db, b_acc.id)

    def test_partial_reversal_refunds_clearance_pro_rata(self, db):
        """部分冲正 100/400：清缴按比例退还 100，成交单置 partial_reversed。"""
        seller, buyer, trade = _settled_market(db, emission=600, qty=400)

        rv = reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="部分冲正核查", quantity=100)
        db.expire_all()
        trade = db.get(AuctionTrade, trade.id)
        assert trade.status == TRADE_PARTIAL_REVERSED
        assert float(trade.reversed_quantity) == approx(100)
        assert float(trade.cleared_refunded_quantity) == approx(100)
        assert float(rv.reversed_quantity) == approx(100)
        assert float(rv.cleared_refund_quantity) == approx(100)

        record = db.query(ComplianceRecord).filter_by(
            company_id=buyer.id, is_active=1).one()
        assert record.status == "deficit"
        assert float(record.cleared_amount) == approx(500)  # 600 - 100
        assert float(record.deficit) == approx(100)

        # 卖方只收回 100
        assert float(_account(db, seller.id).current_balance) == approx(700)
        _assert_snapshot(db, _account(db, seller.id).id)
        _assert_snapshot(db, _account(db, buyer.id).id)

    def test_partial_then_full_consumes_budget_with_no_rounding_residual(self, db):
        """无缺口场景（到账留存为自由可用）下逐笔部分冲正：累计占满预算，
        最后一笔兜底无比例残差，成交单终态 reversed。"""
        seller, buyer, trade = _settled_market(db, emission=0, qty=300, buyer_quota=900)
        reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="部分冲正一", quantity=100)
        db.expire_all()
        reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="部分冲正二", quantity=100)
        db.expire_all()
        trade = db.get(AuctionTrade, trade.id)
        assert trade.status == TRADE_PARTIAL_REVERSED
        reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="最后一笔冲正")
        db.expire_all()
        trade = db.get(AuctionTrade, trade.id)
        assert trade.status == TRADE_REVERSED
        assert float(trade.reversed_quantity) == approx(300)
        # 无清缴联动：归因清缴量与退还量都为 0（验证残差兜底逻辑不凭空产生退还）
        assert float(trade.cleared_quantity) == 0
        assert float(trade.cleared_refunded_quantity) == 0

    def test_partial_then_full_with_clearance_refund_no_residual(self, db):
        """带缺口成交（qty=400 全部用于补缴）：100 + 100 部分冲正各退清缴 100，
        剩余 200 冲正兜底退清缴 200，累计清缴退还恰为归因量，无舍入残差。"""
        seller, buyer, trade = _settled_market(db, emission=600, qty=400)
        # 清算后买方持仓 0：每次冲正先退清缴（产生自由可用）再追回，资金自洽
        r1 = reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="部分冲正一", quantity=100)
        db.expire_all()
        r2 = reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="部分冲正二", quantity=100)
        db.expire_all()
        r3 = reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="剩余冲正")
        db.expire_all()
        trade = db.get(AuctionTrade, trade.id)
        assert trade.status == TRADE_REVERSED
        assert float(trade.cleared_refunded_quantity) == approx(400)
        assert float(r1.cleared_refund_quantity + r2.cleared_refund_quantity
                     + r3.cleared_refund_quantity) == approx(400)
        assert float(r3.cleared_refund_quantity) == approx(200)

    def test_reversal_over_remaining_room_rejected(self, db):
        """回退量超过剩余可回退量直接拒绝，不产生任何流水。"""
        seller, buyer, trade = _settled_market(db, emission=0, qty=400, buyer_quota=600)
        reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="先冲正一部分", quantity=100)
        db.expire_all()
        tx_count = db.query(AllowanceTransaction).count()
        with pytest.raises(AuctionError):
            reverse_settled_trade(
                db, trade.id, kind=REVERSAL_REGULATOR, reason="超量冲正", quantity=400)
        db.rollback()
        assert db.query(AllowanceTransaction).count() == tx_count

    def test_terminal_trade_cannot_reverse_again(self, db):
        seller, buyer, trade = _settled_market(db, emission=0, qty=100, buyer_quota=600)
        reverse_settled_trade(db, trade.id, kind=REVERSAL_REGULATOR, reason="全额冲正")
        db.expire_all()
        with pytest.raises(AuctionError):
            reverse_settled_trade(db, trade.id, kind=REVERSAL_REGULATOR, reason="再次冲正")

    def test_reserved_or_cancelled_trade_not_reversible(self, db):
        """未结算/已撤场成交单不得冲正。"""
        seller = _make_company(db, "S1", "卖方甲", 1000)
        buyer = _make_company(db, "B1", "买方丙", 200)
        db.commit()
        s = create_session(db, year=YEAR, name="t", operator=ADMIN)
        open_session(db, s.id, ADMIN)
        place_bid(db, s.id, seller.id, "sell", 100, 80, operator=ADMIN)
        place_bid(db, s.id, buyer.id, "buy", 100, 90, operator=ADMIN)
        run_matching(db, s.id, ADMIN)
        trade = db.query(AuctionTrade).filter_by(session_id=s.id).one()
        with pytest.raises(AuctionError):
            reverse_settled_trade(db, trade.id, kind=REVERSAL_REGULATOR, reason="未结算冲正")

    def test_reason_required(self, db):
        seller, buyer, trade = _settled_market(db, emission=0, qty=100, buyer_quota=600)
        with pytest.raises(AuctionError):
            reverse_settled_trade(db, trade.id, kind=REVERSAL_REGULATOR, reason="x")

    def test_strict_regulator_reversal_rejected_when_buyer_short(self, db):
        """买方把到账配额用掉后，严格冲正不足额：拒绝且整笔回滚，无任何回退痕迹。"""
        seller, buyer, trade = _settled_market(db, emission=0, qty=400, buyer_quota=200)
        # 结算后买方持仓 600；模拟其后续卖出 500，仅剩 100 自由可用
        acc = _account(db, buyer.id)
        from app.core.ledger import transactional
        with transactional(db):
            lock_row_for_write(db, acc.id)
            apply_ledger_delta(db, acc.id, -500, 0, 0)
        db.expire_all()

        with pytest.raises(AuctionError):
            reverse_settled_trade(
                db, trade.id, kind=REVERSAL_REGULATOR, reason="严格冲正余额不足")
        db.rollback()
        db.expire_all()
        assert db.query(AuctionTradeReversal).count() == 0
        assert db.query(AllowanceTransaction).filter(
            AllowanceTransaction.tx_type.in_(
                ["auction_clearance_reversal", "auction_reversal_clawback",
                 "auction_reversal_return"])).count() == 0
        assert db.get(AuctionTrade, trade.id).status == TRADE_SETTLED
        assert float(db.get(AuctionTrade, trade.id).reversed_quantity) == 0
        # 卖方未被退还
        assert float(_account(db, seller.id).current_balance) == approx(600)


class TestDefaultReversal:
    def test_buyer_default_partial_clawback_books_shortfall(self, db):
        """买方违约且自由可用不足：尽力追回，欠量挂账，成交单置 defaulted。"""
        seller, buyer, trade = _settled_market(db, emission=0, qty=400, buyer_quota=200)
        # 模拟买方结算后另行卖出 500（写真实流水保持快照链可推算），仅剩 100 可用
        from app.core.ledger import transactional
        from app.services.quota_service import _add_ledger_tx
        acc = _account(db, buyer.id)
        with transactional(db):
            acc = lock_row_for_write(db, acc.id)
            bal, frz, rsv = apply_ledger_delta(db, acc.id, -500, 0, 0)
            _add_ledger_tx(
                db, acc, "sell", 500, bal, frz, "二级市场",
                "结算后另行卖出", reserved_after=rsv)
        db.expire_all()

        rv = reverse_settled_trade(
            db, trade.id, kind=REVERSAL_BUYER_DEFAULT, reason="买方未履约违约")
        db.expire_all()
        trade = db.get(AuctionTrade, trade.id)
        assert trade.status == TRADE_DEFAULTED
        assert float(trade.reversed_quantity) == approx(400)  # 预算占满
        assert float(rv.reversed_quantity) == approx(100)
        assert float(rv.defaulted_quantity) == approx(300)
        assert rv.status == "partial"
        # 卖方只收回 100，300 成为违约欠账（配额离开市场口径）
        assert float(_account(db, seller.id).current_balance) == approx(700)
        assert float(_account(db, buyer.id).current_balance) == 0
        _assert_snapshot(db, _account(db, seller.id).id)
        _assert_snapshot(db, _account(db, buyer.id).id)

    def test_buyer_default_with_full_available_completes_clean(self, db):
        """违约回退时买方仍有足额自由可用：等同正常追回，无欠量、状态为 reversed。"""
        seller, buyer, trade = _settled_market(db, emission=0, qty=400, buyer_quota=200)
        rv = reverse_settled_trade(
            db, trade.id, kind=REVERSAL_BUYER_DEFAULT, reason="买方违约但可足额追回")
        db.expire_all()
        assert db.get(AuctionTrade, trade.id).status == TRADE_REVERSED
        assert float(rv.defaulted_quantity) == 0
        assert rv.status == "completed"
        assert float(_account(db, seller.id).current_balance) == approx(1000)

    def test_seller_default_kind_uses_same_ledger_chain(self, db):
        seller, buyer, trade = _settled_market(db, emission=0, qty=100, buyer_quota=600)
        rv = reverse_settled_trade(
            db, trade.id, kind=REVERSAL_SELLER_DEFAULT, reason="卖方资质瑕疵违约")
        db.expire_all()
        assert db.get(AuctionTrade, trade.id).status == TRADE_REVERSED
        assert float(rv.reversed_quantity) == approx(100)
        assert rv.kind == REVERSAL_SELLER_DEFAULT

    def test_default_on_cleared_trade_refunds_clearance_then_claws(self, db):
        """已联动清缴的成交违约：先退还到账补缴（缺口回补），再尽力追回成交配额。"""
        seller, buyer, trade = _settled_market(db, emission=600, qty=400)
        # 清算后买方持仓 0：退还 400 清缴 → 可用 400 → 全额追回 400
        rv = reverse_settled_trade(
            db, trade.id, kind=REVERSAL_BUYER_DEFAULT, reason="买方违约整单回退")
        db.expire_all()
        assert float(rv.cleared_refund_quantity) == approx(400)
        assert float(rv.reversed_quantity) == approx(400)
        assert float(rv.defaulted_quantity) == 0
        record = db.query(ComplianceRecord).filter_by(
            company_id=buyer.id, is_active=1).one()
        assert float(record.deficit) == approx(400)
        assert float(_account(db, seller.id).current_balance) == approx(1000)
        assert float(_account(db, buyer.id).current_balance) == 0


class TestNoClearanceLink:
    def test_reversal_when_auto_clear_disabled(self, db):
        """关闭联动清缴的成交冲正：无清缴退还，纯配额原路返回。"""
        seller, buyer, trade = _settled_market(
            db, emission=600, qty=400, auto_clear=False)
        # 结算后：冻结 200、自由 400（到账留存），缺口 400
        assert float(trade.cleared_quantity) == 0
        rv = reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="关闭联动场次冲正")
        db.expire_all()
        assert float(rv.cleared_refund_quantity) == 0
        assert float(rv.reversed_quantity) == approx(400)
        assert db.query(AllowanceTransaction).filter_by(
            tx_type="auction_clearance_reversal").count() == 0
        assert db.get(AuctionTrade, trade.id).status == TRADE_REVERSED
        # 买方回到结算前：持仓 200 全冻结；卖方回到 1000
        b_acc = _account(db, buyer.id)
        assert float(b_acc.current_balance) == approx(200)
        assert float(b_acc.frozen_balance) == approx(200)
        assert float(_account(db, seller.id).current_balance) == approx(1000)
        _assert_snapshot(db, b_acc.id)

    def test_reversal_without_compliance_record(self, db):
        """买方无履约记录（未批准报告）：清缴退化为空操作，仅配额回退。"""
        seller, buyer, trade = _settled_market(db, emission=0, qty=100, buyer_quota=600)
        rv = reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="无履约记录冲正")
        assert float(rv.cleared_refund_quantity) == 0
        assert db.query(ComplianceRecord).filter_by(company_id=buyer.id).count() == 0


class TestIdempotencyAndConcurrency:
    def test_idempotency_key_returns_first_reversal(self, db):
        seller, buyer, trade = _settled_market(db, emission=0, qty=100, buyer_quota=600)
        r1 = reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="幂等冲正",
            quantity=50, allow_partial=True, idempotency_key="REV-1")
        r2 = reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="幂等冲正",
            quantity=50, allow_partial=True, idempotency_key="REV-1")
        assert r1.id == r2.id
        assert db.query(AuctionTradeReversal).count() == 1
        db.expire_all()
        assert float(db.get(AuctionTrade, trade.id).reversed_quantity) == approx(50)

    def test_concurrent_full_reversals_only_effective_once_budget(self, db):
        """多线程并发全额冲正：累计追回/退还恰为一次成交量。"""
        seller, buyer, trade = _settled_market(db, emission=0, qty=300, buyer_quota=900)
        tid = trade.id
        outcomes = []

        def worker():
            session = _fresh(db)
            try:
                rv = reverse_settled_trade(
                    session, tid, kind=REVERSAL_REGULATOR, reason="并发冲正",
                    idempotency_key=None,
                )
                outcomes.append(("ok", float(rv.reversed_quantity)))
            except AuctionError:
                session.rollback()
                outcomes.append(("reject", 0.0))
            finally:
                session.close()

        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(lambda _: worker(), range(6)))

        db.expire_all()
        ok = [q for kind, q in outcomes if kind == "ok"]
        # 恰好一笔成功且追回 300，其余被成交单预算抢占拒绝
        assert len(ok) == 1
        assert ok == [approx(300)]
        assert db.query(AuctionTradeReversal).count() == 1
        assert db.query(AllowanceTransaction).filter_by(
            tx_type="auction_reversal_clawback").count() == 1
        assert db.query(AllowanceTransaction).filter_by(
            tx_type="auction_reversal_return").count() == 1
        assert float(_account(db, seller.id).current_balance) == approx(1000)
        assert float(_account(db, buyer.id).current_balance) == approx(900)
        assert db.get(AuctionTrade, tid).status == TRADE_REVERSED
        _assert_snapshot(db, _account(db, seller.id).id)
        _assert_snapshot(db, _account(db, buyer.id).id)

    def test_concurrent_partial_reversals_never_exceed_quantity(self, db):
        """多线程各冲正 120（总量 480 > 成交 300）：累计不超过 300，守恒。"""
        seller, buyer, trade = _settled_market(db, emission=0, qty=300, buyer_quota=900)
        tid = trade.id

        def worker(i):
            session = _fresh(db)
            try:
                reverse_settled_trade(
                    session, tid, kind=REVERSAL_REGULATOR, reason=f"并发部分冲正{i}",
                    quantity=120, allow_partial=True, idempotency_key=f"REV-{i}",
                )
                return "ok"
            except AuctionError:
                session.rollback()
                return "reject"
            finally:
                session.close()

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(worker, range(4)))

        db.expire_all()
        assert results.count("ok") == 2  # 120 + 120 = 240 ... 第三笔 60 余量不足整笔
        # 说明：每笔申请 120 且不自动拆分，第三笔因超剩余量被拒
        t = db.get(AuctionTrade, tid)
        assert float(t.reversed_quantity) == approx(240)
        assert t.status == TRADE_PARTIAL_REVERSED
        total_return = sum(
            float(x.amount)
            for x in db.query(AllowanceTransaction).filter_by(
                tx_type="auction_reversal_return")
        )
        assert total_return == approx(240)
        # 市场总量守恒（300 中 240 已退回卖方，60 仍在买方）
        total = sum(float(a.current_balance) for a in db.query(AllowanceAccount))
        assert total == approx(1900)
        _assert_snapshot(db, _account(db, seller.id).id)
        _assert_snapshot(db, _account(db, buyer.id).id)

    def test_concurrent_same_idempotency_key_single_row(self, db):
        """同幂等键并发：唯一约束兜底，只落一张回退单。"""
        seller, buyer, trade = _settled_market(db, emission=0, qty=200, buyer_quota=900)
        tid = trade.id

        def worker():
            session = _fresh(db)
            try:
                reverse_settled_trade(
                    session, tid, kind=REVERSAL_REGULATOR, reason="同键并发冲正",
                    idempotency_key="DUP-KEY",
                )
                return "ok"
            except AuctionError:
                session.rollback()
                return "reject"
            finally:
                session.close()

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: worker(), range(4)))
        db.expire_all()
        assert results.count("ok") >= 1
        assert db.query(AuctionTradeReversal).count() == 1
        assert float(db.get(AuctionTrade, tid).reversed_quantity) == approx(200)

    def test_reversal_races_manual_clear_conservation(self, db):
        """回退与手动清缴并发：同企业年度键串行化，两种时序下配额都守恒、
        流水链自洽——清缴先得则买方已无自由可供追回，严格冲正被拒（配额已
        合法清缴，属预期）；冲正先得则到账配额退回卖方，清缴只能核销冻结。"""
        from app.services.quota_service import clear_emission

        seller, buyer, trade = _settled_market(
            db, emission=600, qty=200, buyer_quota=200, auto_clear=False)
        # 关闭联动：结算后买方 冻结200 + 自由200（到账留存），缺口 400
        tid = trade.id

        def do_reverse():
            session = _fresh(db)
            try:
                reverse_settled_trade(
                    session, tid, kind=REVERSAL_REGULATOR, reason="并发监管冲正",
                    idempotency_key="RACE-REV")
                return "reversed"
            except AuctionError:
                session.rollback()
                return "rejected"
            finally:
                session.close()

        def do_clear():
            session = _fresh(db)
            try:
                clear_emission(session, buyer.id, YEAR, f"{YEAR}-12-31")
            finally:
                session.close()
            return "cleared"

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(lambda f: f(), [do_reverse, do_clear]))

        db.expire_all()
        b_acc = _account(db, buyer.id)
        s_acc = _account(db, seller.id)
        record = db.query(ComplianceRecord).filter_by(
            company_id=buyer.id, year=YEAR, is_active=1).one()
        assert float(b_acc.current_balance) >= 0
        assert float(b_acc.frozen_balance) >= 0
        assert float(record.cleared_amount) <= 600 + 1e-9
        _assert_snapshot(db, b_acc.id)
        _assert_snapshot(db, s_acc.id)

        rev_outcome = outcomes[0]
        if rev_outcome == "reversed":
            # 冲正先执行：到账 200 退回卖方（卖方 1000），清缴只能核销冻结 200
            assert db.get(AuctionTrade, tid).status == TRADE_REVERSED
            assert float(s_acc.current_balance) == approx(1000)
            assert float(record.cleared_amount) == approx(200)
        else:
            # 清缴先执行：冻结200+到账200 全部清缴，买方 0、卖方 800，
            # 成交单维持 settled（严格冲正因无自由可用而拒绝，未产生脏账）
            assert db.get(AuctionTrade, tid).status == TRADE_SETTLED
            assert float(s_acc.current_balance) == approx(800)
            assert float(b_acc.current_balance) == 0
            assert db.query(AuctionTradeReversal).count() == 0


class TestAudit:
    def test_reversal_actions_audited(self, db):
        seller, buyer, trade = _settled_market(db, emission=0, qty=100, buyer_quota=600)
        reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="审计冲正动作", operator=ADMIN)
        logs = list_audit_logs(db)
        actions = {x.action for x in logs}
        assert "trade.reverse" in actions
        entry = next(x for x in logs if x.action == "trade.reverse")
        assert entry.target_type == "trade"
        assert entry.target_id == trade.id
        assert entry.result == "success"
        assert entry.operator_name == "admin"

    def test_default_action_audited(self, db):
        seller, buyer, trade = _settled_market(db, emission=0, qty=100, buyer_quota=600)
        reverse_settled_trade(
            db, trade.id, kind=REVERSAL_BUYER_DEFAULT, reason="审计违约动作", operator=ADMIN)
        logs = list_audit_logs(db)
        assert any(x.action == "trade.default" for x in logs)

    def test_list_reversals_helper(self, db):
        seller, buyer, trade = _settled_market(db, emission=0, qty=200, buyer_quota=900)
        reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="部分一",
            quantity=100, allow_partial=True)
        db.expire_all()
        reverse_settled_trade(
            db, trade.id, kind=REVERSAL_REGULATOR, reason="部分二",
            quantity=100, allow_partial=True)
        rows = list_trade_reversals(db, trade_id=trade.id)
        assert [r.reversal_no for r in rows] == sorted(r.reversal_no for r in rows)
        assert len(rows) == 2
        session_rows = list_trade_reversals(db, session_id=trade.session_id)
        assert len(session_rows) == 2
