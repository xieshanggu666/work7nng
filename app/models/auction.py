from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    text,
)

from app.core.database import Base


class AuctionSession(Base):
    """碳配额集中竞价场次：监管创建 → 开放报价 → 统一撮合 → 集中结算。

    状态机：
    - draft：草稿，监管可编辑公告信息（保留价、品种、时间窗），企业不可见报价入口；
    - open：报价开放，买/卖企业提交密封报价，可撤单；
    - matched：已撮合。按统一出清价生成成交单，卖方对应配额转为交易占用 reserved；
    - settled：已结算。占用配额离开卖方、买方到账，同事务回写流水并核销买方履约缺口；
    - cancelled：草稿/开放期撤场（无账本副作用），或撮合后撤场（逐笔释放卖方占用）。
    """

    __tablename__ = "auction_sessions"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_auction_session_idem"),
    )

    id = Column(Integer, primary_key=True)
    session_no = Column(String(32), nullable=False, unique=True, index=True)
    name = Column(String(128), nullable=False, default="")
    year = Column(Integer, nullable=False, index=True)
    product = Column(String(32), nullable=False, default="allowance")  # 配额品种（预留：allowance/CCER）
    reserve_price = Column(Numeric(18, 2), nullable=False, default=0)  # 保留价：低于该价的卖出不参与撮合
    estimated_volume = Column(Numeric(18, 4), nullable=True)           # 公告拟成交量（仅展示）
    status = Column(String(16), nullable=False, default="draft", index=True)
    clear_price = Column(Numeric(18, 2), nullable=True)                # 撮合成交统一价
    matched_volume = Column(Numeric(18, 4), nullable=False, default=0)
    trade_count = Column(Integer, nullable=False, default=0)
    # 结算时是否用买方到账配额自动核销其同年度履约缺口（默认开启，年度配额闭环）
    auto_clear_deficit = Column(Integer, nullable=False, default=1)
    open_at = Column(DateTime, nullable=True)
    close_at = Column(DateTime, nullable=True)
    matched_at = Column(DateTime, nullable=True)
    settled_at = Column(DateTime, nullable=True)
    cancelled_at = Column(DateTime, nullable=True)
    cancel_reason = Column(String(256), nullable=False, default="")
    cancelled_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    remark = Column(String(256), nullable=False, default="")
    idempotency_key = Column(String(64), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class AuctionBid(Base):
    """集中竞价报价单：买方（求购）/卖方（出让）在开放场次内密封报价。

    状态：
    - active：有效报价，开放期可撤；卖方报量不得超过自由可用配额；
    - matched：撮合成交量等于报价量（全部成交）；
    - partial：部分成交，余量不再参与后续撮合（本场次单次撮合）；
    - unmatched：撮合后未成交（价量不满足出清条件），终态；
    - cancelled：开放期撤单、监管撤单或场次取消，终态。
    """

    __tablename__ = "auction_bids"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_auction_bid_idem"),
        # 同一企业在同一场次同一方向只允许一张有效报价；撤单/成交后该约束自然释放，
        # 允许重新报价。部分唯一索引在 SQLite/PostgreSQL 生效，其他库由场次键锁兜底。
        Index(
            "uq_auction_active_bid",
            "session_id",
            "company_id",
            "side",
            unique=True,
            sqlite_where=text("status = 'active'"),
            postgresql_where=text("status = 'active'"),
        ),
    )

    id = Column(Integer, primary_key=True)
    bid_no = Column(String(32), nullable=False, unique=True, index=True)
    session_id = Column(Integer, ForeignKey("auction_sessions.id"), nullable=False, index=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    side = Column(String(4), nullable=False)  # buy / sell
    year = Column(Integer, nullable=False, index=True)
    quantity = Column(Numeric(18, 4), nullable=False)
    price = Column(Numeric(18, 2), nullable=False, default=0)
    filled_quantity = Column(Numeric(18, 4), nullable=False, default=0)
    # active/matched/partial/unmatched/cancelled
    status = Column(String(16), nullable=False, default="active", index=True)
    tx_date = Column(String(10), nullable=False, default="")
    remark = Column(String(256), nullable=False, default="")
    cancel_reason = Column(String(256), nullable=False, default="")
    cancelled_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    idempotency_key = Column(String(64), nullable=True)
    matched_at = Column(DateTime, nullable=True)
    cancelled_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class AuctionTrade(Base):
    """竞价成交单：撮合时按出清价生成并占用卖方配额，结算时双方账户划转。

    状态：
    - reserved：撮合完成，卖方配额已转为交易占用，等待场次统一结算；
    - settled：已结算，卖方出库 / 买方到账 / 履约缺口核销全部落库；
    - cancelled：撮合后场次被监管撤销，占用已释放，成交单作废。
    """

    __tablename__ = "auction_trades"

    id = Column(Integer, primary_key=True)
    trade_no = Column(String(32), nullable=False, unique=True, index=True)
    session_id = Column(Integer, ForeignKey("auction_sessions.id"), nullable=False, index=True)
    buyer_bid_id = Column(Integer, ForeignKey("auction_bids.id"), nullable=False, index=True)
    seller_bid_id = Column(Integer, ForeignKey("auction_bids.id"), nullable=False, index=True)
    buyer_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    seller_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    year = Column(Integer, nullable=False, index=True)
    quantity = Column(Numeric(18, 4), nullable=False)
    price = Column(Numeric(18, 2), nullable=False, default=0)  # 统一出清价
    alloc_seq = Column(Integer, nullable=False, default=0)     # 价格-时间优先撮合序号
    status = Column(String(16), nullable=False, default="reserved", index=True)
    settled_at = Column(DateTime, nullable=True)
    cancelled_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class AuctionAuditLog(Base):
    """竞价市场权限与操作审计：场次管理、报价/撤单、撮合结算、越权拒绝均留痕。"""

    __tablename__ = "auction_audit_logs"

    id = Column(Integer, primary_key=True)
    operator_id = Column(Integer, nullable=True, index=True)
    operator_name = Column(String(64), nullable=False, default="")
    operator_role = Column(String(16), nullable=False, default="")
    # session.create/open/match/settle/cancel、bid.place/cancel、access.denied
    action = Column(String(64), nullable=False, index=True)
    # session / bid / trade
    target_type = Column(String(16), nullable=False, default="")
    target_id = Column(Integer, nullable=True)
    session_id = Column(Integer, nullable=True, index=True)
    detail = Column(String(500), nullable=False, default="")
    result = Column(String(16), nullable=False, default="success")  # success / denied
    ip = Column(String(64), nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
