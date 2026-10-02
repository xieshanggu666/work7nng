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
    - cancelled：撮合后场次被监管撤销，占用已释放，成交单作废；
    - reversed：结算后监管冲正，成交配额已全部追回并退还卖方（终态）；
    - partial_reversed：结算后部分冲正/违约回退，仍有剩余成交未回退；
    - defaulted：违约回退，买方自由可用不足导致部分配额无法追回，
      未追回量记入回退单 defaulted_qty 作为违约欠缴挂账。

    已结算成交单不允许“无错撤销”，只能由监管冲正/违约回退链路逐笔
    （或部分数量）回退：配额、流水、履约清缴与审计记录在同一事务联动。
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
    # reserved/settled/cancelled/reversed/partial_reversed/defaulted
    status = Column(String(20), nullable=False, default="reserved", index=True)
    settled_at = Column(DateTime, nullable=True)
    # 累计已回退成交量（监管冲正/违约回退共享同一把“成交量预算”，永不超过 quantity）
    reversed_quantity = Column(Numeric(18, 4), nullable=False, default=0)
    # 结算联动清缴中归属于本成交单、由到账配额实际补缴的量（FIFO 归因），
    # 回退时按比例优先退还该部分履约清缴；冻结清缴不属于成交到账，不随冲正回退
    cleared_quantity = Column(Numeric(18, 4), nullable=False, default=0)
    # 其中已经随历次回退退还履约的量（累计，绝不超过 cleared_quantity）
    cleared_refunded_quantity = Column(Numeric(18, 4), nullable=False, default=0)
    last_reversed_at = Column(DateTime, nullable=True)
    cancelled_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class AuctionTradeReversal(Base):
    """已结算成交单的监管冲正/违约回退单。

    一笔成交单可分多次回退（部分回退），每次回退生成一张回退单并同事务：
    1. 按回退量占成交单的比例，优先退还结算时由本成交到账配额补缴的履约清缴
       （回补买方履约缺口、恢复配额状态）；
    2. 从买方自由可用配额追回成交配额（尽力而为，不足部分记 defaulted_qty）；
    3. 实际追回量退还卖方账户并记入卖方流水。

    并发与幂等：
    - 幂等键唯一约束 + 进程内场次/账户/清缴键锁（锁序与结算一致）；
    - 成交单累计回退量用条件 UPDATE 抢占，累计不超过成交量，
      重复提交只返回首张回退单，绝不重复追回/退还。

    kind：
    - regulator_reversal：监管冲正（要求买方自由可用足额覆盖，否则拒绝整笔回滚，
      除非显式允许部分回退 allow_partial）；
    - buyer_default / seller_default：违约回退（买方/卖方违约），
      买方可用不足时自动部分追回，缺口 defaulted_qty 挂账。
    """

    __tablename__ = "auction_trade_reversals"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_auction_trade_reversal_idem"),
    )

    id = Column(Integer, primary_key=True)
    reversal_no = Column(String(32), nullable=False, unique=True, index=True)
    trade_id = Column(Integer, ForeignKey("auction_trades.id"), nullable=False, index=True)
    session_id = Column(Integer, ForeignKey("auction_sessions.id"), nullable=False, index=True)
    buyer_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    seller_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    year = Column(Integer, nullable=False, index=True)
    # regulator_reversal / buyer_default / seller_default
    kind = Column(String(24), nullable=False)
    # 申请回退量（监管可只冲正成交单的一部分）
    request_quantity = Column(Numeric(18, 4), nullable=False)
    # 实际从买方追回并退还卖方的量
    reversed_quantity = Column(Numeric(18, 4), nullable=False, default=0)
    # 其中退还买方履约清缴（回补缺口）的量
    cleared_refund_quantity = Column(Numeric(18, 4), nullable=False, default=0)
    # 买方自由可用不足、未能追回的违约挂账量
    defaulted_quantity = Column(Numeric(18, 4), nullable=False, default=0)
    reason = Column(String(256), nullable=False, default="")
    status = Column(String(16), nullable=False, default="completed")  # completed/partial
    idempotency_key = Column(String(64), nullable=True)
    operator_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    operator_name = Column(String(64), nullable=False, default="")
    tx_date = Column(String(10), nullable=False, default="")
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
