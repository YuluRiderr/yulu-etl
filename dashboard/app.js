// Reads window.OPS_DATA (baked into data.js by build_data.py at CI build
// time — see that file's docstring). Nothing here ever fetches anything;
// the page is fully static and works offline once loaded. The page itself
// reloads every AUTO_REFRESH_MS so a tab left open picks up a fresh
// data.js after the next automatic deploy, without anyone hitting F5.

const BLR_TOTAL_LABEL = "BLR (Total)";
const AUTO_REFRESH_MS = 15 * 60 * 1000;

// Oldest -> newest, left to right, so every column's delta reads as
// "this period vs. the one right before it" and the rightmost (Latest
// Day) column ends up comparing the freshest data against its own most
// recent predecessor — i.e. "latest compared to previous", not the other
// way around.
const PERIOD_KEYS = ["p14_35", "p7_14", "p0_7", "latest"];
const PERIOD_LABELS = { p14_35: "14–35 Days Ago", p7_14: "7–14 Days Ago", p0_7: "0–7 Days Ago", latest: "Latest Day" };

const METRIC_TABS = [
  { key: "dau", label: "DAU", fmt: v => Math.round(v).toLocaleString("en-IN"), unit: "" },
  { key: "service_swap_fulfillment_pct_user", label: "Swap Fulfillment (User)", fmt: v => v.toFixed(1), unit: "%" },
  { key: "service_swap_fulfillment_pct_token", label: "Swap Fulfillment (Token)", fmt: v => v.toFixed(1), unit: "%" },
  { key: "attachment_fulfillment_pct_user", label: "Attach Fulfillment (User)", fmt: v => v.toFixed(1), unit: "%" },
  { key: "attachment_fulfillment_pct_token", label: "Attach Fulfillment (Token)", fmt: v => v.toFixed(1), unit: "%" },
  { key: "mechanic_productivity_90d", label: "Mech. Productivity", fmt: v => v.toFixed(2), unit: "" },
  { key: "enquiry_total", label: "Enquiries (avg/day)", fmt: v => v.toFixed(1), unit: "" },
  { key: "enquiry_to_attachment_pct", label: "Enquiry Conversion", fmt: v => v.toFixed(1), unit: "%" },
];

const DATA = window.OPS_DATA || { anchor_date: null, days_captured: 0, clusters: {}, dates: [], series: {} };
let ACTIVE_TAB = 0;
let ACTIVE_CLUSTER = BLR_TOTAL_LABEL;
const CHART_INSTANCES = [];

function fmtDate(d) {
  if (!d) return "—";
  return new Date(d + "T00:00:00").toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" });
}

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function clusterNames() {
  return Object.keys(DATA.clusters).filter(c => c !== BLR_TOTAL_LABEL).sort(
    (a, b) => (get(b, "dau", "latest") || 0) - (get(a, "dau", "latest") || 0)
  );
}

function allClusterChoices() {
  return [BLR_TOTAL_LABEL, ...clusterNames()];
}

function get(cluster, metric, period) {
  const c = DATA.clusters[cluster];
  if (!c || !c[metric]) return null;
  return c[metric][period];
}

function series(cluster, metric) {
  const c = DATA.series ? DATA.series[cluster] : null;
  if (!c || !c[metric]) return [];
  return c[metric];
}

function kpiTile(label, value, unit, foot) {
  return `<div class="kpi">
    <div class="label">${label}</div>
    <div class="value num">${value}<span class="unit">${unit || ""}</span></div>
    <div class="foot">${foot || ""}</div>
  </div>`;
}

function kpiTileDual(label, aLabel, aVal, bLabel, bVal, unit) {
  const a = aVal !== null && aVal !== undefined ? aVal.toFixed(1) : "—";
  const b = bVal !== null && bVal !== undefined ? bVal.toFixed(1) : "—";
  return `<div class="kpi">
    <div class="label">${label}</div>
    <div class="value num">${a}<span class="unit">${unit}</span></div>
    <div class="foot"><span>${aLabel}</span> · <b class="dot2">${b}${unit}</b> <span>${bLabel}</span></div>
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

// ---------------- sections ----------------

function sectionOverview() {
  const dau = get(BLR_TOTAL_LABEL, "dau", "latest");
  const swapUser = get(BLR_TOTAL_LABEL, "service_swap_fulfillment_pct_user", "latest");
  const swapToken = get(BLR_TOTAL_LABEL, "service_swap_fulfillment_pct_token", "latest");
  const attachUser = get(BLR_TOTAL_LABEL, "attachment_fulfillment_pct_user", "latest");
  const attachToken = get(BLR_TOTAL_LABEL, "attachment_fulfillment_pct_token", "latest");
  const prod = get(BLR_TOTAL_LABEL, "mechanic_productivity_90d", "latest");
  const enq = get(BLR_TOTAL_LABEL, "enquiry_total", "latest");
  const conv = get(BLR_TOTAL_LABEL, "enquiry_to_attachment_pct", "latest");

  return `
    <section id="overview">
      <div class="eyebrow">Weekly Review · ${DATA.days_captured || 0} day(s) of history captured</div>
      <div class="title disp">Daily Ops Review</div>
      <p class="deck-desc">City-wide fleet demand, swap &amp; attachment fulfillment, mechanic productivity and enquiry conversion across every BLR cluster — as of ${fmtDate(DATA.anchor_date)}.</p>
      <div class="kpi-row">
        ${kpiTile("DAU", dau !== null ? Math.round(dau).toLocaleString("en-IN") : "—", "", "riders / day")}
        ${kpiTileDual("Swap Fulfillment", "user", swapUser, "token", swapToken, "%")}
        ${kpiTileDual("Attach Fulfillment", "user", attachUser, "token", attachToken, "%")}
        ${kpiTile("Mech. Productivity", prod !== null ? prod.toFixed(2) : "—", "", "bikes / mechanic (&gt;90d)")}
        ${kpiTile("Enquiries", enq !== null ? Math.round(enq) : "—", "", conv !== null ? `${conv.toFixed(1)}% → attached` : "")}
      </div>
    </section>`;
}

const TREND_CHARTS = [
  { title: "DAU", metrics: [{ key: "dau", label: "DAU", color: "--accent" }] },
  { title: "Swap Fulfillment", metrics: [
      { key: "service_swap_fulfillment_pct_user", label: "User", color: "--accent" },
      { key: "service_swap_fulfillment_pct_token", label: "Token", color: "--accent2" },
    ], unit: "%" },
  { title: "Attach Fulfillment", metrics: [
      { key: "attachment_fulfillment_pct_user", label: "User", color: "--accent" },
      { key: "attachment_fulfillment_pct_token", label: "Token", color: "--accent2" },
    ], unit: "%" },
  { title: "Mechanic Productivity", metrics: [{ key: "mechanic_productivity_90d", label: "bikes / mechanic", color: "--accent" }] },
  { title: "Enquiries", metrics: [{ key: "enquiry_total", label: "Enquiries", color: "--accent" }] },
  { title: "Enquiry → Attachment", metrics: [{ key: "enquiry_to_attachment_pct", label: "Conversion", color: "--accent2" }], unit: "%" },
];

function sectionTrends() {
  const options = allClusterChoices().map(c =>
    `<option value="${c}" ${c === ACTIVE_CLUSTER ? "selected" : ""}>${c}</option>`).join("");

  const cards = TREND_CHARTS.map((chart, i) => {
    const legend = chart.metrics.length > 1
      ? `<div class="legend">${chart.metrics.map(m => `<span><i style="background:var(${m.color})"></i>${m.label}</span>`).join("")}</div>`
      : "";
    return `<div class="chart-card">
      <h4>${chart.title}</h4>
      ${legend}
      <div class="box"><canvas id="chart-${i}"></canvas></div>
    </div>`;
  }).join("");

  return `
    <section id="trends">
      <div class="eyebrow">Trends</div>
      <div class="title disp" style="font-size:28px;">Last ${DATA.dates ? DATA.dates.length : 0} Days</div>
      <p class="deck-desc">Daily values for <b class="num">${ACTIVE_CLUSTER}</b> — pick a cluster to see its own trend instead of the city total.</p>
      <div class="toolbar">
        <select class="cluster-pick" onchange="setCluster(this.value)">${options}</select>
      </div>
      <div class="chart-grid">${cards}</div>
    </section>`;
}

function renderCharts() {
  CHART_INSTANCES.forEach(c => c.destroy());
  CHART_INSTANCES.length = 0;
  if (!DATA.dates || !DATA.dates.length) return;

  const labels = DATA.dates.map(d => {
    const dt = new Date(d + "T00:00:00");
    return dt.toLocaleDateString("en-IN", { day: "2-digit", month: "short" });
  });
  const gridColor = cssVar("--border");
  const textColor = cssVar("--text-dim");

  TREND_CHARTS.forEach((chart, i) => {
    const ctx = document.getElementById(`chart-${i}`);
    if (!ctx) return;
    const datasets = chart.metrics.map(m => ({
      label: m.label,
      data: series(ACTIVE_CLUSTER, m.key),
      borderColor: cssVar(m.color),
      backgroundColor: cssVar(m.color),
      borderWidth: 2,
      pointRadius: 0,
      pointHoverRadius: 3,
      tension: 0.25,
      spanGaps: true,
    }));
    const inst = new Chart(ctx, {
      type: "line",
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: { legend: { display: false }, tooltip: { callbacks: {
          label: (item) => `${item.dataset.label}: ${item.formattedValue}${chart.unit || ""}`,
        } } },
        scales: {
          x: { grid: { display: false }, ticks: { color: textColor, maxTicksLimit: 6, font: { size: 10 } } },
          y: { grid: { color: gridColor }, ticks: { color: textColor, font: { size: 10 } } },
        },
      },
    });
    CHART_INSTANCES.push(inst);
  });
}

function sectionCompare() {
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
    <section id="compare">
      <div class="eyebrow">Period Comparison</div>
      <div class="title disp" style="font-size:28px;">Latest vs. Previous Windows</div>
      <p class="deck-desc">Each column is compared against the one just before it in time, so the rightmost (Latest Day) delta always reads as "latest vs. the most recent prior window".</p>
      ${note}
      <div class="tabs">${tabs}</div>
      <div class="panel" style="overflow-x:auto;">
        <table class="ptable">
          <thead><tr><th>Cluster</th>${PERIOD_KEYS.map(k => `<th>${PERIOD_LABELS[k]}</th>`).join("")}</tr></thead>
          <tbody>${body}</tbody>
        </table>
      </div>
    </section>`;
}

function sectionClusters() {
  const dauRows = clusterNames().map(c => ({ name: c, dau: get(c, "dau", "latest") }));
  const prodRows = clusterNames().map(c => ({ name: c, v: get(c, "mechanic_productivity_90d", "latest") }));
  const enqRows = clusterNames().map(c => ({ name: c, v: get(c, "enquiry_total", "latest") }));
  const convRows = clusterNames().map(c => ({ name: c, v: get(c, "enquiry_to_attachment_pct", "latest") }));
  const cityProd = get(BLR_TOTAL_LABEL, "mechanic_productivity_90d", "latest");

  return `
    <section id="clusters">
      <div class="eyebrow">By Cluster</div>
      <div class="title disp" style="font-size:28px;">Today, Ranked by Cluster</div>
      <p class="deck-desc">Where the fleet is actually being used, and how maintenance/enquiries break down, on ${fmtDate(DATA.anchor_date)}.</p>
      <div class="panel">
        <h3>Daily Active Users</h3>
        ${barChart(dauRows, r => r.dau, { fmt: v => Math.round(v).toLocaleString("en-IN") })}
      </div>
      <div class="panel">
        <h3>Mechanic Productivity — city avg. ${cityProd !== null ? cityProd.toFixed(2) : "—"}</h3>
        ${barChart(prodRows, r => r.v, { fmt: v => v.toFixed(2) })}
      </div>
      <div class="panel">
        <h3>Enquiries logged &amp; conversion to Attachment</h3>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;">
          <div>${barChart(enqRows, r => r.v, { fmt: v => Math.round(v) })}</div>
          <div>${barChart(convRows, r => r.v, { fmt: v => v.toFixed(0) + "%", max: 100 })}</div>
        </div>
      </div>
    </section>`;
}

function computeFlags() {
  const rules = [
    { key: "service_swap_fulfillment_pct_user", label: "Service Swap fulfillment (user)", unit: "%", warn: 6, bad: 12 },
    { key: "service_swap_fulfillment_pct_token", label: "Service Swap fulfillment (token)", unit: "%", warn: 6, bad: 12 },
    { key: "attachment_fulfillment_pct_user", label: "Attachment fulfillment (user)", unit: "%", warn: 6, bad: 12 },
    { key: "attachment_fulfillment_pct_token", label: "Attachment fulfillment (token)", unit: "%", warn: 6, bad: 12 },
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

function sectionAttention() {
  const flags = computeFlags();
  let body;
  if (!DATA.anchor_date) {
    body = `<div class="panel"><div class="empty-note">No data captured yet — this fills in once STEP F's first run lands.</div></div>`;
  } else if (flags.length === 0) {
    body = `<div class="panel"><div class="all-clear">✓ No cluster is meaningfully off city average today.</div></div>`;
  } else {
    const rows = flags.slice(0, 10).map(f => `
      <div class="flag ${f.sev}">
        <div class="fmeta">
          <div class="fcluster">${f.cluster}</div>
          <div class="fdetail">${f.label}: <span class="num">${f.v.toFixed(1)}${f.unit}</span> vs. city <span class="num">${f.cv.toFixed(1)}${f.unit}</span></div>
        </div>
        <div class="fdelta num">−${Math.abs(f.delta).toFixed(1)}${f.unit}</div>
      </div>`).join("");
    body = `<div class="panel"><div class="flag-list">${rows}</div></div>`;
  }
  return `
    <section id="attention">
      <div class="eyebrow">Needs Attention</div>
      <div class="title disp" style="font-size:28px;">What Went Wrong</div>
      <p class="deck-desc">Every cluster trailing the city average by a meaningful margin on ${fmtDate(DATA.anchor_date)}, worst gap first — checked across both user- and token-level fulfillment.</p>
      ${body}
    </section>`;
}

function render() {
  document.getElementById("app").innerHTML = [
    sectionOverview(),
    sectionTrends(),
    sectionCompare(),
    sectionClusters(),
    sectionAttention(),
  ].join("");
  renderCharts();
}

function setTab(i) { ACTIVE_TAB = i; render(); }
function setCluster(c) { ACTIVE_CLUSTER = c; render(); }

document.getElementById("asOfLabel").textContent = fmtDate(DATA.anchor_date);
render();

// Auto-refresh: a tab left open on this static page will otherwise never
// see a newer deploy on its own. Reload periodically so the next build
// (triggered right after every ETL run — see deploy-dashboard.yml) shows
// up without anyone manually hitting refresh.
setTimeout(() => location.reload(), AUTO_REFRESH_MS);
