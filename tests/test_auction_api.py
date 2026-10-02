"""碳配额集中竞价市场 API 集成测试：角色权限、场次流转、密封报价、结算闭环与审计。"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base, get_db
from app.core.security import hash_password
from app.main import app
from app.models import (
    AllowanceAccount,
    AllowanceTransaction,
    AuctionAuditLog,
    AuctionSession,
    Company,
    ComplianceRecord,
    User,
)
from app.services.quota_service import allocate_quota

YEAR = 2026


def _seed_db(TestingSession):
    """在给定 session 工厂上搭建 3 家企业 + 监管/核查账号与年度配额。"""
    db = TestingSession()
    pwd_hash, salt = hash_password("123456")
    c1 = Company(code="A-001", name="卖方企业", industry="电力", region="华东")
    c2 = Company(code="A-002", name="买方企业", industry="水泥", region="华北")
    c3 = Company(code="A-003", name="第三方企业", industry="化工", region="华南")
    db.add_all([c1, c2, c3])
    db.flush()
    db.add_all([
        User(username="s1", display_name="卖方", role="enterprise",
             company_id=c1.id, password_hash=pwd_hash, salt=salt),
        User(username="b1", display_name="买方", role="enterprise",
             company_id=c2.id, password_hash=pwd_hash, salt=salt),
        User(username="e3", display_name="第三方", role="enterprise",
             company_id=c3.id, password_hash=pwd_hash, salt=salt),
        User(username="admin", display_name="监管员", role="admin",
             password_hash=pwd_hash, salt=salt),
        User(username="verifier", display_name="核查员", role="verifier",
             password_hash=pwd_hash, salt=salt),
    ])
    allocate_quota(db, c1.id, YEAR, baseline=1000, allocation_amount=1000)
    allocate_quota(db, c2.id, YEAR, baseline=200, allocation_amount=200)
    allocate_quota(db, c3.id, YEAR, baseline=100, allocation_amount=100)
    db.commit()
    ids = {"c1": c1.id, "c2": c2.id, "c3": c3.id}
    db.close()
    return ids


def _install_engine(engine):
    """为 engine 安装每请求独立 session 的 get_db 覆盖，返回 session 工厂。"""
    TestingSession = sessionmaker(bind=engine, autoflush=False)
    Base.metadata.create_all(engine)

    def override_get_db():
        session = TestingSession()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_get_db
    return TestingSession


@pytest.fixture()
def ctx():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestingSession = _install_engine(engine)
    ids = _seed_db(TestingSession)
    client = TestClient(app)
    yield client, ids
    app.dependency_overrides.clear()
    engine.dispose()


@pytest.fixture()
def file_ctx(tmp_path):
    """文件型多连接库：并发请求各自持有独立连接，真实复现生产并发语义。"""
    engine = create_engine(
        f"sqlite:///{tmp_path / 'auction_api.db'}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )

    @event.listens_for(engine, "connect")
    def _busy_timeout(dbapi_conn, _rec):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA busy_timeout=30000")
        cur.close()

    TestingSession = _install_engine(engine)
    ids = _seed_db(TestingSession)
    client = TestClient(app)
    yield client, ids
    app.dependency_overrides.clear()
    engine.dispose()


def login(client, username):
    client.post("/api/auth/login", json={"username": username, "password": "123456"})


def create_open_session(client, reserve_price=0.0, auto_clear=True):
    login(client, "admin")
    res = client.post("/api/auctions", json={
        "year": YEAR,
        "name": "2026首场集中竞价",
        "reserve_price": reserve_price,
        "auto_clear_deficit": auto_clear,
        "open_at": "2026-03-01T09:00:00",
    })
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["status"] == "open"
    return body


def _setup_buyer_deficit(company_id, emission, factor=0.5703):
    """构造年度缺口：活动数据→核算→报告批准（冻结+缺口）。"""
    from app.models import ActivityData, CalculationMethod, EmissionFactor, EmissionScope
    from app.services.calculation_service import recalc_company_year
    from app.services.mrv_service import approve_report, generate_report, submit_report

    db = next(app.dependency_overrides[get_db]())
    db.add(EmissionScope(company_id=company_id, scope="2", category="外购电力", name="厂区用电"))
    db.add(CalculationMethod(
        method_code="ELEC", name="外购电力排放因子法", scope="2", formula_type="activity_factor"))
    db.add(EmissionFactor(
        factor_code="ELEC-GRID", name="外购电力", scope="2", unit="tCO2/MWh", value=factor,
        source="电网因子", valid_from="2025-01-01", valid_to="2026-12-31"))
    db.flush()
    scope_id = db.query(EmissionScope).filter_by(company_id=company_id, scope="2").one().id
    db.add(ActivityData(
        company_id=company_id, scope_id=scope_id, year=YEAR, period="monthly",
        activity_type="外购电力", unit="MWh",
        quantity=round(emission / factor, 6), data_source="台账", verified=1))
    db.commit()
    recalc_company_year(db, company_id, YEAR)
    report = generate_report(db, company_id, YEAR)
    submit_report(db, report)
    approve_report(db, report, verifier_id=1)
    db.commit()
    db.close()


class TestSessionPermissions:
    def test_enterprise_cannot_manage_session(self, ctx):
        client, _ = ctx
        login(client, "s1")
        # 企业不能建场/开放/撮合/结算/撤场
        assert client.post("/api/auctions", json={"year": YEAR, "name": "x"}).status_code == 403
        assert client.post("/api/auctions/1/open").status_code == 403
        assert client.post("/api/auctions/1/match").status_code == 403
        assert client.post("/api/auctions/1/settle").status_code == 403
        assert client.post("/api/auctions/1/cancel", json={"reason": "x"}).status_code == 403

    def test_verifier_readonly(self, ctx):
        client, _ = ctx
        login(client, "verifier")
        assert client.get("/api/auctions").status_code == 200
        assert client.post("/api/auctions", json={"year": YEAR, "name": "x"}).status_code == 403

    def test_requires_login(self, ctx):
        client, _ = ctx
        client.cookies.clear()
        assert client.get("/api/auctions").status_code == 401
        assert client.get("/api/auctions/my-trades").status_code == 401


class TestFullAuctionFlow:
    def test_create_open_bid_match_settle(self, ctx):
        """监管建场（直接开放）→ 卖方/买方报价 → 撮合 → 结算，账户与状态全闭环。"""
        client, ids = ctx
        session = create_open_session(client)
        sid = session["id"]

        login(client, "s1")
        res = client.post(f"/api/auctions/{sid}/bids", json={
            "side": "sell", "quantity": 300, "price": 80,
        }, headers={"Idempotency-Key": "sell-1"})
        assert res.status_code == 200, res.text
        assert res.json()["status"] == "active"
        # 幂等重试返回同一张报价
        again = client.post(f"/api/auctions/{sid}/bids", json={
            "side": "sell", "quantity": 999, "price": 80,
        }, headers={"Idempotency-Key": "sell-1"})
        assert again.json()["id"] == res.json()["id"]

        # 卖方账户：300 吨转为交易占用
        acc = client.get(f"/api/companies/{ids['c1']}/account?year={YEAR}").json()
        assert acc["current_balance"] == 1000
        assert acc["reserved_balance"] == 300
        assert acc["available_balance"] == 700

        login(client, "b1")
        res = client.post(f"/api/auctions/{sid}/bids", json={
            "side": "buy", "quantity": 300, "price": 90,
        })
        assert res.status_code == 200

        # 企业只能看到本企业报价
        bids = client.get(f"/api/auctions/{sid}/bids").json()
        assert len(bids) == 1
        assert bids[0]["company_id"] == ids["c2"]

        # 监管撮合
        login(client, "admin")
        bids = client.get(f"/api/auctions/{sid}/bids").json()
        assert len(bids) == 2
        res = client.post(f"/api/auctions/{sid}/match")
        assert res.status_code == 200
        matched = res.json()
        assert matched["status"] == "matched"
        # 两档成交量相同（300），未匹配量同为 0，按集合竞价惯例取均价 (80+90)/2
        assert matched["clear_price"] == 85
        assert matched["matched_volume"] == 300
        assert matched["trade_count"] == 1

        # 监管结算
        res = client.post(f"/api/auctions/{sid}/settle")
        assert res.status_code == 200
        assert res.json()["status"] == "settled"

        login(client, "s1")
        acc = client.get(f"/api/companies/{ids['c1']}/account?year={YEAR}").json()
        assert acc["current_balance"] == 700
        assert acc["reserved_balance"] == 0
        login(client, "b1")
        acc = client.get(f"/api/companies/{ids['c2']}/account?year={YEAR}").json()
        assert acc["current_balance"] == 500

        # 企业 my-trades 隔离
        trades = client.get("/api/auctions/my-trades").json()
        assert len(trades) == 1
        assert trades[0]["buyer_id"] == ids["c2"]

        # 流水带 auction_trade_id
        db = next(app.dependency_overrides[get_db]())
        txs = db.query(AllowanceTransaction).filter(
            AllowanceTransaction.tx_type.in_(["auction_deliver_in", "auction_deliver_out"])
        ).all()
        assert len(txs) == 2
        assert all(t.auction_trade_id is not None for t in txs)
        db.close()

    def test_bid_only_in_open_session(self, ctx):
        client, _ = ctx
        # 先建草稿
        login(client, "admin")
        sid = client.post("/api/auctions", json={"year": YEAR, "name": "草稿场"}).json()["id"]
        login(client, "s1")
        res = client.post(f"/api/auctions/{sid}/bids", json={"side": "sell", "quantity": 100, "price": 80})
        assert res.status_code == 400
        assert "开放" in res.json()["detail"]

    def test_sell_over_available_rejected(self, ctx):
        client, _ = ctx
        sid = create_open_session(client)["id"]
        login(client, "s1")
        res = client.post(f"/api/auctions/{sid}/bids", json={"side": "sell", "quantity": 5000, "price": 80})
        assert res.status_code == 400
        assert "自由可用" in res.json()["detail"]

    def test_duplicate_side_rejected(self, ctx):
        client, _ = ctx
        sid = create_open_session(client)["id"]
        login(client, "s1")
        client.post(f"/api/auctions/{sid}/bids", json={"side": "sell", "quantity": 100, "price": 80})
        res = client.post(f"/api/auctions/{sid}/bids", json={"side": "sell", "quantity": 100, "price": 81})
        assert res.status_code == 400
        assert "已有有效报价" in res.json()["detail"]

    def test_enterprise_cancel_own_and_below_reserve(self, ctx):
        client, _ = ctx
        sid = create_open_session(client, reserve_price=88)["id"]
        login(client, "s1")
        # 低于保留价拒绝
        res = client.post(f"/api/auctions/{sid}/bids", json={"side": "sell", "quantity": 100, "price": 80})
        assert res.status_code == 400
        bid = client.post(f"/api/auctions/{sid}/bids", json={"side": "sell", "quantity": 100, "price": 88}).json()
        # 撤单后占用释放
        res = client.post(f"/api/auctions/bids/{bid['id']}/cancel", json={"reason": "改主意"})
        assert res.status_code == 200
        assert res.json()["status"] == "cancelled"
        acc = client.get("/api/companies/1/account" if False else f"/api/companies/{bid['company_id']}/account?year={YEAR}").json()
        assert acc["reserved_balance"] == 0
        assert acc["available_balance"] == 1000


class TestAuthzAndAudit:
    def test_other_company_cancel_denied_and_audited(self, ctx):
        client, ids = ctx
        sid = create_open_session(client)["id"]
        login(client, "s1")
        bid = client.post(f"/api/auctions/{sid}/bids", json={"side": "sell", "quantity": 100, "price": 80}).json()
        # 第三方企业撤他人报价：403 且审计落库
        login(client, "e3")
        res = client.post(f"/api/auctions/bids/{bid['id']}/cancel", json={"reason": "恶意"})
        assert res.status_code == 403

        login(client, "admin")
        logs = client.get("/api/auctions/audit-logs").json()
        denied = [x for x in logs if x["result"] == "denied" and x["action"] == "bid.cancel"]
        assert len(denied) == 1
        assert denied[0]["operator_name"] == "e3"

    def test_enterprise_cannot_read_audit(self, ctx):
        client, ids = ctx
        create_open_session(client)
        login(client, "s1")
        res = client.get("/api/auctions/audit-logs")
        assert res.status_code == 403
        # 越权读取审计本身也被审计
        login(client, "admin")
        logs = client.get("/api/auctions/audit-logs").json()
        assert any(x["action"] == "audit.read" and x["result"] == "denied" for x in logs)

    def test_enterprise_cannot_read_all_trades(self, ctx):
        client, _ = ctx
        create_open_session(client)
        login(client, "s1")
        assert client.get("/api/auctions/trades/all").status_code == 403
        login(client, "verifier")
        assert client.get("/api/auctions/trades/all").status_code == 200

    def test_regulator_cancel_any_bid(self, ctx):
        client, _ = ctx
        sid = create_open_session(client)["id"]
        login(client, "s1")
        bid = client.post(f"/api/auctions/{sid}/bids", json={"side": "sell", "quantity": 100, "price": 80}).json()
        login(client, "admin")
        res = client.post(f"/api/auctions/bids/{bid['id']}/cancel", json={"reason": "监管撤单"})
        assert res.status_code == 200
        assert res.json()["status"] == "cancelled"
        logs = client.get("/api/auctions/audit-logs").json()
        assert any(x["action"] == "bid.cancel" and x["operator_name"] == "admin" for x in logs)


class TestSettlementLoop:
    def test_settle_closes_buyer_deficit(self, ctx):
        """买方缺口经竞价结算到账自动补缴，接口返回后履约达标。"""
        client, ids = ctx
        # 买方排放 600：持仓 200 全冻结，缺口 400
        _setup_buyer_deficit(ids["c2"], 600)
        sid = create_open_session(client)["id"]

        login(client, "s1")
        client.post(f"/api/auctions/{sid}/bids", json={"side": "sell", "quantity": 400, "price": 80})
        login(client, "b1")
        client.post(f"/api/auctions/{sid}/bids", json={"side": "buy", "quantity": 400, "price": 90})
        login(client, "admin")
        client.post(f"/api/auctions/{sid}/match")
        res = client.post(f"/api/auctions/{sid}/settle")
        assert res.status_code == 200

        compliance = client.get(f"/api/compliance?year={YEAR}").json()
        assert len(compliance) == 1
        assert compliance[0]["status"] == "compliant"
        assert compliance[0]["cleared_amount"] == 600

        login(client, "b1")
        acc = client.get(f"/api/companies/{ids['c2']}/account?year={YEAR}").json()
        assert acc["current_balance"] == 0

    def test_cancel_open_session_releases_bid_reservation(self, ctx):
        """开放期撤场：卖方报价占用全部释放。"""
        client, ids = ctx
        sid = create_open_session(client)["id"]
        login(client, "s1")
        client.post(f"/api/auctions/{sid}/bids", json={"side": "sell", "quantity": 300, "price": 80})
        login(client, "admin")
        res = client.post(f"/api/auctions/{sid}/cancel", json={"reason": "暂停交易"})
        assert res.status_code == 200
        assert res.json()["status"] == "cancelled"

        login(client, "s1")
        acc = client.get(f"/api/companies/{ids['c1']}/account?year={YEAR}").json()
        assert acc["reserved_balance"] == 0
        assert acc["available_balance"] == 1000

    def test_match_no_cross_then_settle_empty(self, ctx):
        """买卖价格不交叉：撮合无成交，卖出占用释放，空结算成功。"""
        client, ids = ctx
        sid = create_open_session(client)["id"]
        login(client, "s1")
        client.post(f"/api/auctions/{sid}/bids", json={"side": "sell", "quantity": 100, "price": 95})
        login(client, "b1")
        client.post(f"/api/auctions/{sid}/bids", json={"side": "buy", "quantity": 100, "price": 80})
        login(client, "admin")
        m = client.post(f"/api/auctions/{sid}/match").json()
        assert m["matched_volume"] == 0
        assert m["clear_price"] is None
        assert client.post(f"/api/auctions/{sid}/settle").json()["status"] == "settled"
        login(client, "s1")
        acc = client.get(f"/api/companies/{ids['c1']}/account?year={YEAR}").json()
        assert acc["reserved_balance"] == 0

    def test_concurrent_settle_idempotent(self, file_ctx):
        """并发点击结算：多连接真实并发下只有一次划转，其余幂等或竞争失败。"""
        from concurrent.futures import ThreadPoolExecutor

        client, ids = file_ctx
        sid = create_open_session(client)["id"]
        login(client, "s1")
        client.post(f"/api/auctions/{sid}/bids", json={"side": "sell", "quantity": 300, "price": 80})
        login(client, "b1")
        client.post(f"/api/auctions/{sid}/bids", json={"side": "buy", "quantity": 300, "price": 90})
        login(client, "admin")
        client.post(f"/api/auctions/{sid}/match")

        def fire():
            c = TestClient(app)
            c.post("/api/auth/login", json={"username": "admin", "password": "123456"})
            r = c.post(f"/api/auctions/{sid}/settle")
            # 赢家返回 settled；输家在状态抢占下返回 400（场次状态已变化）
            return r.status_code, r.json().get("status"), r.json().get("detail")

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: fire(), range(4)))
        statuses = [(code, status) for code, status, _ in results]
        assert any(s == "settled" and code == 200 for code, s in statuses)
        assert all(s in ("settled", None) for _, s in statuses)

        db = next(app.dependency_overrides[get_db]())
        assert db.query(AllowanceTransaction).filter_by(tx_type="auction_deliver_in").count() == 1
        assert db.query(AllowanceTransaction).filter_by(tx_type="auction_deliver_out").count() == 1
        db.close()


# --------------------------------------------------------------------------- #
# 已结算成交：监管冲正 / 违约回退 API
# --------------------------------------------------------------------------- #

def _settled_trade(client, ids, *, qty=300, with_deficit=False):
    """走完整流程并返回 (trade_id, session_id)。with_deficit 先给买方造缺口。"""
    if with_deficit:
        # 买方初始 200，排放 500 → 冻结 200、缺口 300，到账 300 恰好足额补缴
        _setup_buyer_deficit(ids["c2"], qty + 200)
    sid = create_open_session(client)["id"]
    login(client, "s1")
    client.post(f"/api/auctions/{sid}/bids", json={"side": "sell", "quantity": qty, "price": 80})
    login(client, "b1")
    client.post(f"/api/auctions/{sid}/bids", json={"side": "buy", "quantity": qty, "price": 90})
    login(client, "admin")
    client.post(f"/api/auctions/{sid}/match")
    res = client.post(f"/api/auctions/{sid}/settle")
    assert res.status_code == 200
    trades = client.get("/api/auctions/trades/all").json()
    trade = next(t for t in trades if t["session_id"] == sid)
    return trade["id"], sid


class TestTradeReversalApi:
    def test_admin_full_reversal_restores_ledger(self, ctx):
        """监管全额冲正：买卖双方配额回退、清缴退还、成交单终态 reversed。"""
        client, ids = ctx
        tid, sid = _settled_trade(client, ids, qty=300, with_deficit=True)

        res = client.post(f"/api/auctions/trades/{tid}/reverse", json={
            "reason": "监管核查发现异常，整单冲正",
        }, headers={"Idempotency-Key": "REV-1"})
        assert res.status_code == 200, res.text
        body = res.json()
        assert body["kind"] == "regulator_reversal"
        assert body["status"] == "completed"
        assert body["reversed_quantity"] == 300
        assert body["cleared_refund_quantity"] == 300
        assert body["defaulted_quantity"] == 0

        # 幂等重放返回同一张回退单
        again = client.post(f"/api/auctions/trades/{tid}/reverse", json={
            "reason": "监管核查发现异常，整单冲正",
        }, headers={"Idempotency-Key": "REV-1"})
        assert again.json()["id"] == body["id"]

        trade = next(t for t in client.get("/api/auctions/trades/all").json()
                     if t["id"] == tid)
        assert trade["status"] == "reversed"
        assert trade["reversed_quantity"] == 300

        # 卖方收回 300（600 结算后 + 300 退还 = 900... 卖方初始 1000，结算划出 300 → 700）
        login(client, "s1")
        s_acc = client.get(f"/api/companies/{ids['c1']}/account?year={YEAR}").json()
        assert s_acc["current_balance"] == 1000

    def test_partial_reversal_then_remainder(self, ctx):
        """部分冲正 100（无缺口，买方足额）：partial_reversed；再冲剩余 200 终结。"""
        client, ids = ctx
        tid, sid = _settled_trade(client, ids, qty=300)

        r1 = client.post(f"/api/auctions/trades/{tid}/reverse", json={
            "reason": "部分冲正第一批", "quantity": 100,
        })
        assert r1.status_code == 200
        assert r1.json()["status"] == "completed"
        trade = next(t for t in client.get("/api/auctions/trades/all").json()
                     if t["id"] == tid)
        assert trade["status"] == "partial_reversed"
        assert trade["reversed_quantity"] == 100

        r2 = client.post(f"/api/auctions/trades/{tid}/reverse", json={"reason": "冲正剩余"})
        assert r2.status_code == 200
        assert r2.json()["request_quantity"] == 200
        trade = next(t for t in client.get("/api/auctions/trades/all").json()
                     if t["id"] == tid)
        assert trade["status"] == "reversed"

        rows = client.get(f"/api/auctions/trades/{tid}/reversals").json()
        assert len(rows) == 2

    def test_strict_reversal_short_buyer_returns_400_and_audits(self, ctx):
        """买方把到账配额卖出致自由可用不足：严格冲正 400 拒绝并留审计，
        状态与账本不变；违约回退放行并挂账。"""
        from app.core.ledger import apply_ledger_delta, lock_row_for_write, transactional
        from app.models import AllowanceAccount
        from app.services.quota_service import _add_ledger_tx

        client, ids = ctx
        tid, sid = _settled_trade(client, ids, qty=300)  # 无缺口：买方到账后持仓 500
        db = next(app.dependency_overrides[get_db]())
        acc = db.query(AllowanceAccount).filter_by(company_id=ids["c2"], year=YEAR).one()
        with transactional(db):
            acc = lock_row_for_write(db, acc.id)
            bal, frz, rsv = apply_ledger_delta(db, acc.id, -450, 0, 0)  # 仅余 50
            _add_ledger_tx(db, acc, "sell", 450, bal, frz, "二级市场", "后续卖出",
                           reserved_after=rsv)
        db.close()

        res = client.post(f"/api/auctions/trades/{tid}/reverse", json={
            "reason": "买方余额不足时严格冲正",
        })
        assert res.status_code == 400
        assert "自由可用" in res.json()["detail"]

        trade = next(t for t in client.get("/api/auctions/trades/all").json()
                     if t["id"] == tid)
        assert trade["status"] == "settled"
        assert trade["reversed_quantity"] == 0
        logs = client.get("/api/auctions/audit-logs?limit=50").json()
        denied = [l for l in logs if l["action"] == "trade.reverse" and l["result"] == "denied"]
        assert denied, "冲正被拒必须留审计"

        # 显式允许部分冲正则放行（追回 50）
        ok = client.post(f"/api/auctions/trades/{tid}/reverse", json={
            "reason": "改为部分冲正", "allow_partial": True,
        })
        assert ok.status_code == 200
        assert ok.json()["reversed_quantity"] == 50
        assert ok.json()["defaulted_quantity"] == 250

    def test_buyer_default_partial_books_shortfall(self, ctx):
        """买方违约：结算后买方把配额卖出致可用不足，违约回退部分追回+欠量挂账。"""
        from app.core.ledger import apply_ledger_delta, lock_row_for_write, transactional
        from app.models import AllowanceAccount
        from app.services.quota_service import _add_ledger_tx

        client, ids = ctx
        tid, sid = _settled_trade(client, ids, qty=300)  # 买方到账后持仓 500
        db = next(app.dependency_overrides[get_db]())
        acc = db.query(AllowanceAccount).filter_by(company_id=ids["c2"], year=YEAR).one()
        with transactional(db):
            acc = lock_row_for_write(db, acc.id)
            bal, frz, rsv = apply_ledger_delta(db, acc.id, -400, 0, 0)
            _add_ledger_tx(db, acc, "sell", 400, bal, frz, "二级市场", "后续卖出",
                           reserved_after=rsv)
        db.close()

        res = client.post(f"/api/auctions/trades/{tid}/default", json={
            "reason": "买方资金链断裂违约", "side": "buyer",
        })
        assert res.status_code == 200, res.text
        body = res.json()
        assert body["kind"] == "buyer_default"
        assert body["status"] == "partial"
        assert body["reversed_quantity"] == 100
        assert body["defaulted_quantity"] == 200
        trade = next(t for t in client.get("/api/auctions/trades/all").json()
                     if t["id"] == tid)
        assert trade["status"] == "defaulted"

    def test_seller_default_requires_side(self, ctx):
        client, ids = ctx
        tid, sid = _settled_trade(client, ids, qty=100)
        bad = client.post(f"/api/auctions/trades/{tid}/default", json={
            "reason": "缺少违约方", "side": "exchange",
        })
        assert bad.status_code == 422
        ok = client.post(f"/api/auctions/trades/{tid}/default", json={
            "reason": "卖方资质违约", "side": "seller",
        })
        assert ok.status_code == 200
        assert ok.json()["kind"] == "seller_default"

    def test_enterprise_and_verifier_forbidden(self, ctx):
        """企业与核查员均不能发起冲正/违约回退（403）。"""
        client, ids = ctx
        tid, sid = _settled_trade(client, ids, qty=100)
        login(client, "b1")
        assert client.post(f"/api/auctions/trades/{tid}/reverse",
                           json={"reason": "企业尝试冲正"}).status_code == 403
        assert client.post(f"/api/auctions/trades/{tid}/default",
                           json={"reason": "企业尝试违约", "side": "buyer"}).status_code == 403
        login(client, "verifier")
        assert client.post(f"/api/auctions/trades/{tid}/reverse",
                           json={"reason": "核查员尝试冲正"}).status_code == 403

    def test_enterprise_can_only_read_own_trade_reversals(self, ctx):
        """企业可查本企业成交的回退记录，查他人成交 403 并审计；全量列表 403。"""
        client, ids = ctx
        tid, sid = _settled_trade(client, ids, qty=100)
        client.post(f"/api/auctions/trades/{tid}/reverse", json={"reason": "监管冲正留痕"})

        login(client, "b1")
        assert client.get(f"/api/auctions/trades/{tid}/reversals").status_code == 200
        assert client.get("/api/auctions/reversals/all").status_code == 403

        login(client, "e3")  # 第三方企业
        assert client.get(f"/api/auctions/trades/{tid}/reversals").status_code == 403

        login(client, "admin")
        rows = client.get("/api/auctions/reversals/all").json()
        assert len(rows) == 1
        logs = client.get("/api/auctions/audit-logs?limit=50").json()
        denied = [l for l in logs if l["result"] == "denied" and l["action"] == "trade.read"]
        assert len(denied) >= 2

    def test_reverse_unknown_trade_404(self, ctx):
        client, _ = ctx
        login(client, "admin")
        res = client.post("/api/auctions/trades/99999/reverse", json={"reason": "不存在的成交"})
        assert res.status_code == 404

    def test_concurrent_reverse_only_one_succeeds(self, file_ctx):
        """HTTP 多连接并发全额冲正：只有一次追回/退还，其余被预算抢占拒绝。"""
        from concurrent.futures import ThreadPoolExecutor

        client, ids = file_ctx
        tid, sid = _settled_trade(client, ids, qty=300)

        def fire(i):
            c = TestClient(app)
            c.post("/api/auth/login", json={"username": "admin", "password": "123456"})
            r = c.post(f"/api/auctions/trades/{tid}/reverse",
                       json={"reason": f"并发冲正{i}"},
                       headers={"Idempotency-Key": f"CONC-REV-{i}"})
            return r.status_code

        with ThreadPoolExecutor(max_workers=4) as pool:
            codes = list(pool.map(fire, range(4)))
        assert codes.count(200) == 1
        assert sorted(set(codes)) == [200, 400]

        db = next(app.dependency_overrides[get_db]())
        from app.models import AuctionTradeReversal
        assert db.query(AuctionTradeReversal).count() == 1
        assert db.query(AllowanceTransaction).filter_by(
            tx_type="auction_reversal_clawback").count() == 1
        db.close()
