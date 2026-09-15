// Reads window.OPS_DATA (baked into data.js by build_data.py at CI build
// time — see that file's docstring). Nothing here ever fetches anything;
// the page is fully static and works offline once loaded.

const BLR_TOTAL_LABEL = "BLR (Total)";
const PERIOD_KEYS = ["latest", "p0_7", "p7_14", "p14_35"];
const PERIOD_LABELS = { latest: "Latest Day", p0_7: "0–7 Days", p7_14: "7–14 Days", p14_35: "14–35 Days" };

const METRIC_TABS = [
  { key: "dau", label: "DAU", fmt: v => Math.round(v).toLocaleString("en-IN"), unit: "" },
  { key: "service_swap_fulfillment_pct_token", label: "Swap Fulfillment", fmt: v => v.toFixed(1), unit: "%" },
  { key: "attachment_fulfillment_pct_token", label: "Attach Fulfillment", fmt: v => v.toFixed(1), unit: "%" },
  { key: "mechanic_productivity_90d", label: "Mech. Productivity", fmt: v => v.toFixed(2), unit: "" },
  { key: "enquiry_to_attachment_pct", label: "Enquiry Conversion", fmt: v => v.toFixed(1), unit: "%" },
];

const DATA = window.OPS_DATA || { anchor_date: null, days_captured: 0, clusters: {} };
let SLIDE_IDX = 0;
let ACTIVE_TAB = 0;

function fmtDate(d) {
  if (!d) return "—";
  return new Date(d + "T00:00:00").toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" });
}

function clusterNames() {
  return Object.keys(DATA.clusters).filter(c => c !== BLR_TOTAL_LABEL).sort(
    (a, b) => (get(b, "dau", "latest") || 0) - (get(a, "dau", "latest") || 0)
  );
}

function get(cluster, metric, period) {
  const c = DATA.clusters[cluster];
  if (!c || !c[metric]) return null;
  return c[metric][period];
}

function kpiTile(label, value, unit, foot) {
  return `<div class="kpi">
    <div class="label">${label}</div>
    <div class="value num">${value}<span class="unit">${unit || ""}</span></div>
    <div class="foot">${foot || ""}</div>
  </div>`;
}

function barChart(rows, valueGetter, { fmt = v => v, max = null } = {}) {
  const vals = rows.map(valueGetter).filter(v => v !== null && v !== undefined);
  const m = max ?? Math.max(1, ...vals);
  return rows.map(r => {
    const v = valueGetter(r);
    const pct = (v === null || v === undefined) ? 0 : Math.max(2, (v / m) * 100);
    const label = (v === null || v === undefined) ? "—" : fmt(v);
    return `<div class="bar-row">
      <div class="lab">${r.name}</div>
      <div class="track"><div class="fill" style="width:${pct}%"></div></div>
      <div class="val num">${label}</div>
    </div>`;
  }).join("");
}

// ---------------- slides ----------------

function slideCover() {
  const dau = get(BLR_TOTAL_LABEL, "dau", "latest");
  const swap = get(BLR_TOTAL_LABEL, "service_swap_fulfillment_pct_token", "latest");
  const attach = get(BLR_TOTAL_LABEL, "attachment_fulfillment_pct_token", "latest");
  const prod = get(BLR_TOTAL_LABEL, "mechanic_productivity_90d", "latest");
  const enq = get(BLR_TOTAL_LABEL, "enquiry_total", "latest");
  const conv = get(BLR_TOTAL_LABEL, "enquiry_to_attachment_pct", "latest");

  return `
    <div class="eyebrow">Weekly Review · ${DATA.days_captured || 0} day(s) of history captured</div>
    <div class="title disp">Daily Ops Review</div>
    <p class="deck-desc">City-wide fleet demand, swap &amp; attachment fulfillment, mechanic productivity and enquiry conversion across every BLR cluster — as of ${fmtDate(DATA.anchor_date)}.</p>
    <div class="kpi-row" style="margin-top:auto;">
      ${kpiTile("DAU", dau !== null ? Math.round(dau).toLocaleString("en-IN") : "—", "", "riders / day")}
      ${kpiTile("Swap Fulfillment", swap !== null ? swap.toFixed(1) : "—", "%", "token-level")}
      ${kpiTile("Attach Fulfillment", attach !== null ? attach.toFixed(1) : "—", "%", "token-level")}
      ${kpiTile("Mech. Productivity", prod !== null ? prod.toFixed(2) : "—", "", "bikes / mechanic (>90d)")}
      ${kpiTile("Enquiries", enq !== null ? Math.round(enq) : "—", "", conv !== null ? `${conv.toFixed(1)}% → attached` : "")}
    </div>`;
}

function slidePeriodCompare() {
  const tabs = METRIC_TABS.map((t, i) =>
    `<button class="tab ${i === ACTIVE_TAB ? "on" : ""}" onclick="setTab(${i})">${t.label}</button>`
  ).join("");

  const metric = METRIC_TABS[ACTIVE_TAB];
  const rows = [BLR_TOTAL_LABEL, ...clusterNames()];

  const body = rows.map(cluster => {
    const cells = PERIOD_KEYS.map((pk, i) => {
      const v = get(cluster, metric.key, pk);
      if (v === null || v === undefined) return `<td class="dash">–</td>`;
      let deltaHtml = "";
      if (i > 0) {
        const prev = get(cluster, metric.key, PERIOD_KEYS[i - 1]);
        if (prev !== null && prev !== undefined && prev !== 0) {
          const pct = ((v - prev) / Math.abs(prev)) * 100;
          const dir = pct >= 0 ? "up" : "down";
          const arrow = pct >= 0 ? "▲" : "▼";
          deltaHtml = `<span class="delta ${dir}">${arrow}${Math.abs(pct).toFixed(0)}%</span>`;
        }
      }
      return `<td class="num">${metric.fmt(v)}${metric.unit}${deltaHtml}</td>`;
    }).join("");
    return `<tr class="${cluster === BLR_TOTAL_LABEL ? "total" : ""}"><td>${cluster}</td>${cells}</tr>`;
  }).join("");

  const noteDays = DATA.days_captured || 0;
  const note = noteDays < 8
    ? `<p class="deck-desc">Only ${noteDays} day(s) of history captured so far — trailing-period columns fill in automatically as STEP F keeps running daily.</p>`
    : "";

  return `
    <div class="eyebrow">Period Comparison</div>
    <div class="title disp" style="font-size:32px;">Latest vs. Trailing Windows</div>
    ${note}
    <div class="tabs">${tabs}</div>
    <div class="panel" style="overflow-x:auto;">
      <table class="ptable">
        <thead><tr><th>Cluster</th>${PERIOD_KEYS.map(k => `<th>${PERIOD_LABELS[k]}</th>`).join("")}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>`;
}

function slideDemand() {
  const rows = clusterNames().map(c => ({ name: c, dau: get(c, "dau", "latest") }));
  return `
    <div class="eyebrow">Demand</div>
    <div class="title disp" style="font-size:32px;">Daily Active Users by Cluster</div>
    <p class="deck-desc">Where the fleet is actually being used on ${fmtDate(DATA.anchor_date)} — ranked, largest cluster first.</p>
    <div class="panel">${barChart(rows, r => r.dau, { fmt: v => Math.round(v).toLocaleString("en-IN") })}</div>`;
}

function slideProductivity() {
  const rows = clusterNames().map(c => ({ name: c, v: get(c, "mechanic_productivity_90d", "latest") }));
  const enqRows = clusterNames().map(c => ({ name: c, v: get(c, "enquiry_total", "latest") }));
  const convRows = clusterNames().map(c => ({ name: c, v: get(c, "enquiry_to_attachment_pct", "latest") }));
  const cityProd = get(BLR_TOTAL_LABEL, "mechanic_productivity_90d", "latest");

  return `
    <div class="eyebrow">Maintenance</div>
    <div class="title disp" style="font-size:32px;">Mechanic Productivity</div>
    <p class="deck-desc">Regular-repair bikes + (live-repair bikes ÷ 3), per Maintenance-profile mechanic with &gt;90 days tenure.</p>
    <div class="panel">
      <h3>Bikes serviced per eligible mechanic — city avg. ${cityProd !== null ? cityProd.toFixed(2) : "—"}</h3>
      ${barChart(rows, r => r.v, { fmt: v => v.toFixed(2) })}
    </div>
    <div class="panel">
      <h3>Enquiries logged &amp; conversion to Attachment</h3>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;">
        <div>${barChart(enqRows, r => r.v, { fmt: v => Math.round(v) })}</div>
        <div>${barChart(convRows, r => r.v, { fmt: v => v.toFixed(0) + "%", max: 100 })}</div>
      </div>
    </div>`;
}

function computeFlags() {
  const rules = [
    { key: "service_swap_fulfillment_pct_token", label: "Service Swap fulfillment", unit: "%", warn: 6, bad: 12 },
    { key: "attachment_fulfillment_pct_token", label: "Attachment fulfillment", unit: "%", warn: 6, bad: 12 },
    { key: "mechanic_productivity_90d", label: "Mechanic productivity", unit: " bikes/mech", warn: 1.5, bad: 3 },
    { key: "enquiry_to_attachment_pct", label: "Enquiry→Attachment conv.", unit: "%", warn: 8, bad: 16 },
  ];
  const flags = [];
  clusterNames().forEach(cluster => {
    rules.forEach(rule => {
      const v = get(cluster, rule.key, "latest");
      const cv = get(BLR_TOTAL_LABEL, rule.key, "latest");
      if (v === null || cv === null) return;
      const delta = cv - v;
      if (delta >= rule.bad) flags.push({ cluster, ...rule, v, cv, delta, sev: "bad" });
      else if (delta >= rule.warn) flags.push({ cluster, ...rule, v, cv, delta, sev: "warn" });
    });
  });
  return flags.sort((a, b) => b.delta - a.delta);
}

function slideFlags() {
  const flags = computeFlags();
  if (!DATA.anchor_date) {
    return `
      <div class="eyebrow">Needs Attention</div>
      <div class="title disp" style="font-size:32px;">What Went Wrong</div>
      <div class="panel"><div class="empty-note">No data captured yet — this fills in once STEP F's first run lands.</div></div>`;
  }
  if (flags.length === 0) {
    return `
      <div class="eyebrow">Needs Attention</div>
      <div class="title disp" style="font-size:32px;">What Went Wrong</div>
      <div class="panel"><div class="all-clear">✓ No cluster is meaningfully off city average today.</div></div>`;
  }
  const rows = flags.slice(0, 8).map(f => `
    <div class="flag ${f.sev}">
      <div class="fmeta">
        <div class="fcluster">${f.cluster}</div>
        <div class="fdetail">${f.label}: <span class="num">${f.v.toFixed(1)}${f.unit}</span> vs. city <span class="num">${f.cv.toFixed(1)}${f.unit}</span></div>
      </div>
      <div class="fdelta num">−${Math.abs(f.delta).toFixed(1)}${f.unit}</div>
    </div>`).join("");
  return `
    <div class="eyebrow">Needs Attention</div>
    <div class="title disp" style="font-size:32px;">What Went Wrong</div>
    <p class="deck-desc">Every cluster trailing the city average by a meaningful margin on ${fmtDate(DATA.anchor_date)}, worst gap first.</p>
    <div class="panel"><div class="flag-list">${rows}</div></div>`;
}

const SLIDES = [
  { name: "Cover", render: slideCover },
  { name: "Period Comparison", render: slidePeriodCompare },
  { name: "Demand", render: slideDemand },
  { name: "Productivity", render: slideProductivity },
  { name: "Needs Attention", render: slideFlags },
];

function renderSlide() {
  document.getElementById("stage").innerHTML = `<div class="slide">${SLIDES[SLIDE_IDX].render()}</div>`;
  document.getElementById("prevBtn").disabled = SLIDE_IDX === 0;
  document.getElementById("nextBtn").disabled = SLIDE_IDX === SLIDES.length - 1;
  document.getElementById("prevName").textContent = SLIDE_IDX > 0 ? "← " + SLIDES[SLIDE_IDX - 1].name : "";
  document.getElementById("nextName").textContent = SLIDE_IDX < SLIDES.length - 1 ? SLIDES[SLIDE_IDX + 1].name + " →" : "";
  document.getElementById("dots").innerHTML = SLIDES.map((s, i) =>
    `<button class="dot ${i === SLIDE_IDX ? "on" : ""}" onclick="jump(${i})" aria-label="${s.name}"></button>`).join("");
}

function go(delta) {
  const next = SLIDE_IDX + delta;
  if (next < 0 || next >= SLIDES.length) return;
  SLIDE_IDX = next;
  renderSlide();
}
function jump(i) { SLIDE_IDX = i; renderSlide(); }
function setTab(i) { ACTIVE_TAB = i; renderSlide(); }

document.addEventListener("keydown", e => {
  if (e.key === "ArrowRight") go(1);
  if (e.key === "ArrowLeft") go(-1);
});

document.getElementById("asOfLabel").textContent = fmtDate(DATA.anchor_date);
renderSlide();
