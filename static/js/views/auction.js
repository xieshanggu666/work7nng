window.views = window.views || {};

const auctionStatusMap = {
  draft: ["muted", "草稿"],
  open: ["info", "报价开放中"],
  matched: ["warn", "已撮合待结算"],
  settled: ["ok", "已结算"],
  cancelled: ["danger", "已撤场"],
};

const bidStatusMap = {
  active: ["info", "有效"],
  matched: ["ok", "全部成交"],
  partial: ["warn", "部分成交"],
  unmatched: ["muted", "未成交"],
  cancelled: ["danger", "已撤销"],
};

const auctionTradeStatusMap = {
  reserved: ["warn", "待结算"],
  settled: ["ok", "已结算"],
  cancelled: ["danger", "已作废"],
  reversed: ["muted", "已冲正"],
  partial_reversed: ["warn", "部分回退"],
  defaulted: ["danger", "违约欠缴"],
};

const actionTextMap = {
  "session.create": "创建场次",
  "session.open": "开放报价",
  "session.match": "撮合",
  "session.settle": "结算",
  "session.cancel": "撤场",
  "bid.place": "提交报价",
  "bid.cancel": "撤销报价",
  "trade.reverse": "监管冲正",
  "trade.default": "违约回退",
  "trade.read": "读取全量成交",
  "audit.read": "读取审计",
  "access.denied": "越权访问",
};

views.AuctionView = () => {
  const user = window.__user;
  const isAdmin = user.role === "admin";
  const isRegulator = user.role === "admin" || user.role === "verifier";

  const [sessions, setSessions] = React.useState([]);
  const [selected, setSelected] = React.useState(null); // 选中场次详情
  const [bids, setBids] = React.useState([]);
  const [trades, setTrades] = React.useState([]);
  const [logs, setLogs] = React.useState([]);
  const [myTrades, setMyTrades] = React.useState([]);
  const [reversals, setReversals] = React.useState([]);
  const [tab, setTab] = React.useState("sessions"); // sessions / bids / trades / audit
  const [selYear, setSelYear] = React.useState(2026);
  const [form, setForm] = React.useState({
    name: "", year: 2026, reserve_price: 70, estimated_volume: "",
    autoClear: true, directOpen: true,
  });
  const [bidForm, setBidForm] = React.useState({ side: "buy", quantity: "", price: "" });
  const [msg, setMsg] = React.useState({ type: "", text: "" });
  const [busy, setBusy] = React.useState(false);

  const loadSessions = React.useCallback(async () => {
    const params = new URLSearchParams();
    if (selYear) params.set("year", selYear);
    try {
      setSessions(await api.get(`/api/auctions?${params.toString()}`));
    } catch (e) { setMsg({ type: "err", text: e.message }); }
  }, [selYear]);

  const loadDetail = React.useCallback(async (s) => {
    setSelected(s);
    try {
      const [b, t] = await Promise.all([
        api.get(`/api/auctions/${s.id}/bids`),
        api.get("/api/auctions/my-trades?session_id=" + s.id),
      ]);
      setBids(b);
      setMyTrades(t);
      if (isRegulator) {
        const [allT, l, rv] = await Promise.all([
          api.get(`/api/auctions/trades/all?session_id=${s.id}`),
          api.get(`/api/auctions/audit-logs?session_id=${s.id}`),
          api.get(`/api/auctions/reversals/all?session_id=${s.id}`).catch(() => []),
        ]);
        setTrades(allT);
        setLogs(l);
        setReversals(rv);
      } else {
        setTrades([]);
        setLogs([]);
        setReversals([]);
      }
    } catch (e) { setMsg({ type: "err", text: e.message }); }
  }, [isRegulator]);

  React.useEffect(() => { loadSessions(); }, [loadSessions]);

  const set = (k) => (e) => setForm({ ...form, [k]: e.target.value });
  const setBid = (k) => (e) => setBidForm({ ...bidForm, [k]: e.target.value });

  const refresh = async (type, text) => {
    setMsg(text ? { type, text } : { type: "", text: "" });
    const list = await api.get(`/api/auctions?year=${selYear || ""}`);
    setSessions(list);
    if (selected) {
      const fresh = list.find((x) => x.id === selected.id);
      if (fresh) loadDetail(fresh);
      else setSelected(null);
    }
  };

  const createSession = async (e) => {
    e.preventDefault();
    if (busy) return;
    setBusy(true);
    try {
      const body = {
        name: form.name || `${form.year}年度集中竞价`,
        year: Number(form.year),
        reserve_price: Number(form.reserve_price || 0),
        estimated_volume: form.estimated_volume ? Number(form.estimated_volume) : null,
        auto_clear_deficit: form.autoClear,
      };
      if (form.directOpen) {
        const now = new Date();
        body.open_at = now.toISOString().slice(0, 19);
      }
      const s = await api.post("/api/auctions", body, api.idemKey());
      refresh("ok", `场次 ${s.session_no} 已${s.status === "open" ? "创建并开放" : "创建为草稿"}`);
      setForm({ ...form, name: "", estimated_volume: "" });
    } catch (err) { setMsg({ type: "err", text: err.message }); }
    finally { setBusy(false); }
  };

  const sessionAction = async (s, action, label, bodyObj) => {
    try {
      const r = await api.post(`/api/auctions/${s.id}/${action}`, bodyObj || {}, api.idemKey());
      let extra = "";
      if (action === "match") {
        if (r.matched_volume > 0) {
          extra = `：出清价 ${fmtNum(r.clear_price)} 元/吨，成交 ${r.trade_count} 笔 / ${fmtNum(r.matched_volume, 4)} 吨`;
        } else {
          extra = "：无满足条件的买卖报价，零成交";
        }
      }
      refresh("ok", `场次 ${r.session_no} ${label}成功${extra}，当前状态：${auctionStatusMap[r.status][1]}`);
    } catch (err) { setMsg({ type: "err", text: err.message }); }
  };

  const cancelSession = (s) => {
    const reason = prompt(`撤销场次 ${s.session_no} 的原因（开放/撮合后撤场将释放占用配额）：`, "");
    if (reason === null) return;
    sessionAction(s, "cancel", "撤场", { reason });
  };

  const placeBid = async (e) => {
    e.preventDefault();
    if (!selected || busy) return;
    setBusy(true);
    try {
      const r = await api.post(`/api/auctions/${selected.id}/bids`, {
        side: bidForm.side,
        quantity: Number(bidForm.quantity),
        price: Number(bidForm.price || 0),
      }, api.idemKey());
      refresh("ok", `报价单 ${r.bid_no} 已提交（${bidForm.side === "buy" ? "买入" : "卖出"} ${fmtNum(r.quantity, 4)} 吨 @ ${fmtNum(r.price)}）`);
      setBidForm({ ...bidForm, quantity: "", price: "" });
    } catch (err) { setMsg({ type: "err", text: err.message }); }
    finally { setBusy(false); }
  };

  const cancelBid = async (b) => {
    if (!confirm(`确认撤销报价单 ${b.bid_no}（${b.side === "buy" ? "买入" : "卖出"} ${fmtNum(b.quantity, 4)} 吨）？`)) return;
    try {
      await api.post(`/api/auctions/bids/${b.id}/cancel`, { reason: "企业主动撤单" }, api.idemKey());
      refresh("ok", `报价单 ${b.bid_no} 已撤销${b.side === "sell" ? "，占用配额已释放" : ""}`);
    } catch (err) { setMsg({ type: "err", text: err.message }); }
  };

  const reverseTrade = async (t) => {
    const reason = prompt(`监管冲正成交单 ${t.trade_no} 的原因（将联动回退配额/流水/履约清缴）：`, "");
    if (reason === null || reason.trim().length < 2) return;
    const partial = confirm(
      "确定 → 整单冲正（买方自由可用不足时将拒绝）\n取消 → 部分冲正（尽力追回，不足记违约欠账）"
    );
    let qty = null;
    if (!partial) {
      const input = prompt(`部分冲正数量（吨，剩余可回退 ${fmtNum(t.quantity - (t.reversed_quantity || 0), 4)} 吨）：`, "");
      if (input === null) return;
      qty = Number(input);
      if (!(qty > 0)) { setMsg({ type: "err", text: "冲正数量必须为正数" }); return; }
    }
    try {
      const r = await api.post(`/api/auctions/trades/${t.id}/reverse`, {
        reason: reason.trim(), quantity: qty, allow_partial: !partial,
      }, api.idemKey());
      refresh("ok", `成交单 ${t.trade_no} 冲正完成：追回退还 ${fmtNum(r.reversed_quantity, 4)} 吨`
        + (r.cleared_refund_quantity ? `，退还履约清缴 ${fmtNum(r.cleared_refund_quantity, 4)} 吨` : "")
        + (r.defaulted_quantity ? `，违约欠账 ${fmtNum(r.defaulted_quantity, 4)} 吨` : ""));
    } catch (err) { setMsg({ type: "err", text: err.message }); }
  };

  const defaultTrade = async (t) => {
    const reason = prompt(`违约回退成交单 ${t.trade_no} 的原因：`, "");
    if (reason === null || reason.trim().length < 2) return;
    const side = prompt("违约方（buyer=买方 / seller=卖方）：", "buyer");
    if (side !== "buyer" && side !== "seller") { setMsg({ type: "err", text: "违约方只能是 buyer 或 seller" }); return; }
    let qty = null;
    const input = prompt(`回退数量（吨，留空回退全部剩余 ${fmtNum(t.quantity - (t.reversed_quantity || 0), 4)} 吨）：`, "");
    if (input && input.trim()) qty = Number(input);
    try {
      const r = await api.post(`/api/auctions/trades/${t.id}/default`, {
        reason: reason.trim(), side, quantity: qty,
      }, api.idemKey());
      refresh("ok", `成交单 ${t.trade_no} 违约回退：追回 ${fmtNum(r.reversed_quantity, 4)} 吨`
        + (r.defaulted_quantity ? `，欠缴挂账 ${fmtNum(r.defaulted_quantity, 4)} 吨` : ""));
    } catch (err) { setMsg({ type: "err", text: err.message }); }
  };

  const fmtTime = (v) => v ? new Date(v).toLocaleString("zh-CN", { hour12: false }) : "-";

  return html`
    <div class="panel">
      <h3>竞价场次</h3>
      <div class="filter-bar">
        <div class="field"><label>年度</label>
          <select value=${selYear} onChange=${(e) => setSelYear(Number(e.target.value))}>
            ${[2026, 2025, 2024, 2023].map((y) => html`<option value=${y}>${y}</option>`)}
          </select>
        </div>
        ${isRegulator && html`
          <div class="field" style=${{flexDirection: "row", alignItems: "center", gap: 6}}>
            <button class="btn ghost sm" onClick=${() => setTab(tab === "audit" ? "sessions" : "audit")}>
              ${tab === "audit" ? "返回场次" : "查看权限审计"}
            </button>
          </div>`}
      </div>

      ${tab === "audit" && isRegulator ? html`
        <div>
          <h4>权限与操作审计（全部场次）</h4>
          <AuditLogs />
        </div>` : html`
      <table>
        <thead><tr>
          <th>场次号</th><th>名称</th><th>年度</th><th>保留价</th><th>状态</th>
          <th>出清价</th><th>成交量 (t)</th><th>成交笔数</th><th>开放时间</th><th>操作</th>
        </tr></thead>
        <tbody>
          ${sessions.map((s) => html`
            <tr key=${s.id} style=${{cursor: "pointer", background: selected && selected.id === s.id ? "rgba(47,158,110,0.08)" : ""}}
              onClick=${() => { setSelected(s); loadDetail(s); }}>
              <td class="mono">${s.session_no}</td>
              <td>${s.name || "-"}</td>
              <td>${s.year}</td>
              <td>${fmtNum(s.reserve_price)} 元</td>
              <td>${html([StatusBadge(s.status)])}</td>
              <td>${s.clear_price != null ? fmtNum(s.clear_price) + " 元" : "-"}</td>
              <td style=${{fontWeight: 600}}>${fmtNum(s.matched_volume, 4)}</td>
              <td>${s.trade_count}</td>
              <td>${fmtTime(s.open_at)}</td>
              <td style=${{whiteSpace: "nowrap"}} onClick=${(e) => e.stopPropagation()}>
                <button class="btn sm ghost" onClick=${() => { setSelected(s); loadDetail(s); }}>详情</button>
                ${isAdmin && s.status === "draft" && html`
                  <button class="btn sm" style=${{marginLeft: 6}} onClick=${() => sessionAction(s, "open", "开放报价")}>开放</button>`}
                ${isAdmin && s.status === "open" && html`
                  <button class="btn sm" style=${{marginLeft: 6}} onClick=${() => sessionAction(s, "match", "撮合")}>撮合</button>
                  <button class="btn sm danger" style=${{marginLeft: 6}} onClick=${() => cancelSession(s)}>撤场</button>`}
                ${isAdmin && s.status === "matched" && html`
                  <button class="btn sm" style=${{marginLeft: 6}} onClick=${() => sessionAction(s, "settle", "结算")}>结算</button>
                  <button class="btn sm danger" style=${{marginLeft: 6}} onClick=${() => cancelSession(s)}>撤场</button>`}
              </td>
            </tr>`)}
          ${sessions.length === 0 && html`<tr><td colspan="10" class="empty">暂无竞价场次</td></tr>`}
        </tbody>
      </table>`}
    </div>

    ${isAdmin && tab !== "audit" && html`
    <div class="panel">
      <h3>创建竞价场次</h3>
      <form class="form-grid" onSubmit=${createSession}>
        <div class="field"><label>场次名称</label><input value=${form.name} onChange=${set("name")} placeholder="如：2026年度首期集中竞价" /></div>
        <div class="field"><label>年度</label>
          <select value=${form.year} onChange=${set("year")}>
            ${[2026, 2025, 2024].map((y) => html`<option value=${y}>${y}</option>`)}
          </select>
        </div>
        <div class="field"><label>保留价（元/吨）</label><input type="number" min="0" step="0.01" value=${form.reserve_price} onChange=${set("reserve_price")} /></div>
        <div class="field"><label>拟成交量（t，可选）</label><input type="number" min="0" step="0.0001" value=${form.estimated_volume} onChange=${set("estimated_volume")} /></div>
        <div class="field"><label>履约联动</label>
          <label style=${{display: "flex", alignItems: "center", gap: 6, fontWeight: "normal"}}>
            <input type="checkbox" checked=${form.autoClear} onChange=${(e) => setForm({ ...form, autoClear: e.target.checked })} />
            结算到账自动核销买方同年度缺口
          </label>
        </div>
        <div class="field"><label>创建方式</label>
          <label style=${{display: "flex", alignItems: "center", gap: 6, fontWeight: "normal"}}>
            <input type="checkbox" checked=${form.directOpen} onChange=${(e) => setForm({ ...form, directOpen: e.target.checked })} />
            创建后立即开放报价
          </label>
        </div>
        <div class="actions"><button class="btn" type="submit" disabled=${busy}>${busy ? "提交中…" : "创建场次"}</button></div>
      </form>
      <div class="empty" style=${{textAlign: "left", marginTop: 8}}>
        集中竞价流程：监管建场 → 买/卖方在开放期密封报价（卖出报价立即冻结对应可用配额）→
        监管统一撮合（最大成交量定价，价格-时间优先配对）→ 集中结算（双方配额账户与流水落账，
        同事务自动核销买方履约缺口）。撮合前可撤场（释放全部报价占用），撮合后撤场逐笔释放成交占用。
      </div>
    </div>`}

    ${selected && tab !== "audit" && html`
    <div class="panel">
      <h3>场次 ${selected.session_no} · 报价与成交</h3>
      <div class="cards">
        <div class="card"><div class="label">场次状态</div><div class="value" style=${{fontSize: 18}}>${html([StatusBadge(selected.status)])}</div></div>
        <div class="card"><div class="label">保留价 / 出清价</div>
          <div class="value" style=${{fontSize: 18}}>${fmtNum(selected.reserve_price)} / ${selected.clear_price != null ? fmtNum(selected.clear_price) : "-"}</div>
          <div class="sub">单位：元/吨</div></div>
        <div class="card"><div class="label">成交量 / 有效报价</div>
          <div class="value" style=${{fontSize: 18}}>${fmtNum(selected.matched_volume, 4)}</div>
          <div class="sub">${selected.trade_count} 笔成交，${selected.bid_count} 张有效报价</div></div>
      </div>

      ${selected.status === "open" && html`
      <form class="form-grid" onSubmit=${placeBid} style=${{marginBottom: 16}}>
        <div class="field"><label>方向</label>
          <select value=${bidForm.side} onChange=${setBid("side")}>
            <option value="buy">买方 · 求购配额</option>
            <option value="sell">卖方 · 出让配额</option>
          </select>
        </div>
        <div class="field"><label>数量 (t)</label>
          <input type="number" min="0" step="0.0001" value=${bidForm.quantity} onChange=${setBid("quantity")} required /></div>
        <div class="field"><label>报价 (元/吨)</label>
          <input type="number" min="0" step="0.01" value=${bidForm.price} onChange=${setBid("price")} required /></div>
        <div class="actions" style=${{alignSelf: "end"}}>
          <button class="btn" type="submit" disabled=${busy}>${busy ? "提交中…" : "密封报价"}</button>
        </div>
      </form>
      <div class="empty" style=${{textAlign: "left", marginTop: 0, marginBottom: 12}}>
        提示：卖出报价提交后对应数量立即转为<b>交易占用</b>（持仓不变、不可重复卖出/被履约冻结），
        撤单或未成交时自动释放；同一企业同场次同方向仅允许一张有效报价。
      </div>`}

      <h4>${isRegulator ? "全部报价" : "本企业报价"}</h4>
      <table>
        <thead><tr>
          <th>报价单号</th>${isRegulator ? html`<th>企业</th>` : ""}<th>方向</th><th>数量 (t)</th><th>报价</th>
          <th>已成交 (t)</th><th>状态</th><th>时间</th><th>操作</th>
        </tr></thead>
        <tbody>
          ${bids.map((b) => {
            const own = !isRegulator || user.company_id === b.company_id;
            const canCancel = b.status === "active" && (isAdmin || own);
            return html`
            <tr key=${b.id}>
              <td class="mono">${b.bid_no}</td>
              ${isRegulator ? html`<td>${b.company_name}${user.company_id === b.company_id ? "（我）" : ""}</td>` : ""}
              <td>${b.side === "buy" ? html`<span style=${{color: "var(--blue)"}}>买入</span>` : html`<span style=${{color: "var(--amber)"}}>卖出</span>`}</td>
              <td>${fmtNum(b.quantity, 4)}</td>
              <td>${fmtNum(b.price)} 元</td>
              <td>${fmtNum(b.filled_quantity, 4)}</td>
              <td>${html([StatusBadge(b.status)])}${b.cancel_reason ? html`<div class="muted" style=${{fontSize: 12}}>${b.cancel_reason}</div>` : ""}</td>
              <td>${fmtTime(b.created_at)}</td>
              <td>${canCancel ? html`<button class="btn sm ghost" onClick=${() => cancelBid(b)}>撤销</button>` : html`<span class="muted">-</span>`}</td>
            </tr>`;
          })}
          ${bids.length === 0 && html`<tr><td colspan="9" class="empty">暂无报价</td></tr>`}
        </tbody>
      </table>

      <h4 style=${{marginTop: 18}}>${isRegulator ? "成交明细（全市场）" : "本企业成交"}</h4>
      <table>
        <thead><tr>
          <th>成交单号</th><th>卖方</th>${isRegulator ? "" : ""}<th>买方</th><th>数量 (t)</th><th>成交价</th>
          <th>序号</th><th>状态</th><th>已回退 (t)</th><th>结算时间</th>${isAdmin ? html`<th>监管操作</th>` : ""}
        </tr></thead>
        <tbody>
          ${(isRegulator ? trades : myTrades).map((t) => html`
            <tr key=${t.id}>
              <td class="mono">${t.trade_no}</td>
              <td>${t.seller_name}${user.company_id === t.seller_id ? "（我）" : ""}</td>
              <td>${t.buyer_name}${user.company_id === t.buyer_id ? "（我）" : ""}</td>
              <td>${fmtNum(t.quantity, 4)}</td>
              <td>${fmtNum(t.price)} 元</td>
              <td>${t.alloc_seq}</td>
              <td>${html([StatusBadge(t.status)])}</td>
              <td>${t.reversed_quantity ? fmtNum(t.reversed_quantity, 4) : "-"}</td>
              <td>${fmtTime(t.settled_at)}</td>
              ${isAdmin ? html`<td style=${{whiteSpace: "nowrap"}}>
                ${{
                  settled: html`<button class="btn sm ghost" onClick=${() => reverseTrade(t)}>冲正</button>
                    <button class="btn sm danger" style=${{marginLeft: 4}} onClick=${() => defaultTrade(t)}>违约回退</button>`,
                  partial_reversed: html`<button class="btn sm ghost" onClick=${() => reverseTrade(t)}>继续冲正</button>
                    <button class="btn sm danger" style=${{marginLeft: 4}} onClick=${() => defaultTrade(t)}>违约回退</button>`,
                  defaulted: html`<button class="btn sm danger" onClick=${() => defaultTrade(t)}>继续追讨</button>`,
                }[t.status] || html`<span class="muted">-</span>`}
              </td>` : ""}
            </tr>`)}
          ${(isRegulator ? trades : myTrades).length === 0 && html`<tr><td colspan="10" class="empty">暂无成交</td></tr>`}
        </tbody>
      </table>

      ${isRegulator && reversals.length > 0 && html`
      <h4 style=${{marginTop: 18}}>冲正 / 违约回退记录（本场次）</h4>
      <table>
        <thead><tr>
          <th>回退单号</th><th>成交单</th><th>类型</th><th>卖方→买方</th>
          <th>申请 (t)</th><th>追回退还 (t)</th><th>退还清缴 (t)</th><th>欠缴挂账 (t)</th>
          <th>状态</th><th>操作人</th><th>时间</th><th>原因</th>
        </tr></thead>
        <tbody>
          ${reversals.map((r) => html`
            <tr key=${r.id}>
              <td class="mono">${r.reversal_no}</td>
              <td class="mono">${r.trade_no}</td>
              <td>${{ regulator_reversal: "监管冲正", buyer_default: "买方违约", seller_default: "卖方违约" }[r.kind] || r.kind}</td>
              <td>${r.seller_name}→${r.buyer_name}</td>
              <td>${fmtNum(r.request_quantity, 4)}</td>
              <td>${fmtNum(r.reversed_quantity, 4)}</td>
              <td>${r.cleared_refund_quantity ? fmtNum(r.cleared_refund_quantity, 4) : "-"}</td>
              <td>${r.defaulted_quantity ? html`<span style=${{color: "var(--red)"}}>${fmtNum(r.defaulted_quantity, 4)}</span>` : "-"}</td>
              <td>${r.status === "completed" ? html`<span class="badge ok">完成</span>` : html`<span class="badge danger">部分追回</span>`}</td>
              <td>${r.operator_name || "-"}</td>
              <td>${r.tx_date || fmtTime(r.created_at)}</td>
              <td style=${{maxWidth: 220}}>${r.reason}</td>
            </tr>`)}
        </tbody>
      </table>`}

      ${isRegulator && html`
      <h4 style=${{marginTop: 18}}>本场次操作审计</h4>
      <AuditLogs inline=${true} logs=${logs} />`}
    </div>`}

    ${msg.text && html`<div class="msg ${msg.type}">${msg.text}</div>`}
  `;
};

// 独立审计列表组件：inline 模式使用父级已加载的 logs，否则自行拉取全量
const AuditLogs = ({ inline, logs: injected }) => {
  const [logs, setLogs] = React.useState(injected || []);
  React.useEffect(() => {
    if (!inline) api.get("/api/auctions/audit-logs?limit=300").then(setLogs).catch(() => {});
  }, []);
  const data = inline ? injected : logs;
  return html`
    <table>
      <thead><tr><th>时间</th><th>操作人</th><th>角色</th><th>动作</th><th>对象</th><th>结果</th><th>IP</th><th>详情</th></tr></thead>
      <tbody>
        ${data.map((l) => html`
          <tr key=${l.id}>
            <td class="mono" style=${{whiteSpace: "nowrap"}}>${new Date(l.created_at).toLocaleString("zh-CN", { hour12: false })}</td>
            <td>${l.operator_name}${l.operator_id ? ` (#${l.operator_id})` : ""}</td>
            <td>${l.operator_role || "-"}</td>
            <td>${actionTextMap[l.action] || l.action}</td>
            <td class="mono">${l.target_type}${l.target_id ? "/" + l.target_id : ""}${l.session_id ? " · 场次" + l.session_id : ""}</td>
            <td>${l.result === "success"
              ? html`<span class="badge ok">成功</span>`
              : html`<span class="badge danger">拒绝</span>`}</td>
            <td class="mono">${l.ip || "-"}</td>
            <td style=${{maxWidth: 360}}>${l.detail}</td>
          </tr>`)}
        ${data.length === 0 && html`<tr><td colspan="8" class="empty">暂无审计记录</td></tr>`}
      </tbody>
    </table>`;
};
