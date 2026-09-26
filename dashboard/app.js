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
const PERIOD_KEYS = ["p21_28", "p14_21", "p7_14", "p0_7", "latest"];
const PERIOD_LABELS = {
  p21_28: "21–28 Days Ago", p14_21: "14–21 Days Ago", p7_14: "7–14 Days Ago",
  p0_7: "0–7 Days Ago", latest: "Latest Day",
};

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
// Yulu-Centre-wise fulfillment breakdown -- a separate, additive object
// nested under DATA.by_centre by build_data.py; same shape as DATA
// itself (anchor_date/days_captured/clusters/dates/series), just keyed
// by individual centre name instead of cluster.
const BYCENTRE = DATA.by_centre || { anchor_date: null, days_captured: 0, clusters: {}, dates: [], series: {} };

const CENTRE_METRIC_TABS = [
  { key: "service_swap_fulfillment_pct_user", label: "Swap Fulfillment (User)", fmt: v => v.toFixed(1), unit: "%" },
  { key: "service_swap_fulfillment_pct_token", label: "Swap Fulfillment (Token)", fmt: v => v.toFixed(1), unit: "%" },
  { key: "attachment_fulfillment_pct_user", label: "Attach Fulfillment (User)", fmt: v => v.toFixed(1), unit: "%" },
  { key: "attachment_fulfillment_pct_token", label: "Attach Fulfillment (Token)", fmt: v => v.toFixed(1), unit: "%" },
];

let ACTIVE_TAB = 0;
let ACTIVE_CENTRE_TAB = 0;
let ACTIVE_CLUSTER = BLR_TOTAL_LABEL;
// "By Cluster" date RANGE -- both resolved lazily in sectionClusters()
// (default: the single most recent captured date, i.e. a 1-day "range").
let ACTIVE_CLUSTERS_START = null;
let ACTIVE_CLUSTERS_END = null;
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

function centreNames() {
  return Object.keys(BYCENTRE.clusters).filter(c => c !== BLR_TOTAL_LABEL).sort();
}

function getCentre(centre, metric, period) {
  const c = BYCENTRE.clusters[centre];
  if (!c || !c[metric]) return null;
  return c[metric][period];
}

function series(cluster, metric) {
  const c = DATA.series ? DATA.series[cluster] : null;
  if (!c || !c[metric]) return [];
  return c[metric];
}

// Averages a metric over [startDate, endDate] (inclusive) using the
// daily series (DATA.dates/DATA.series) -- NOT the named-period
// comparison object (which only has "latest"/"p0_7"/etc, never an
// arbitrary range). A single-day range (startDate === endDate) just
// returns that one day's value, so this covers both the old "pick one
// date" behaviour and a true multi-day average. Dates with no row
// (null) are skipped rather than treated as 0, same "average over
// whatever was actually captured" rule build_data.py already uses for
// the named periods.
function getForRange(cluster, metric, startDate, endDate) {
  const dates = DATA.dates || [];
  const arr = series(cluster, metric);
  const vals = [];
  dates.forEach((d, i) => {
    if (d >= startDate && d <= endDate) {
      const v = arr[i];
      if (v !== null && v !== undefined) vals.push(v);
    }
  });
  if (!vals.length) return null;
  return vals.reduce((a, b) => a + b, 0) / vals.length;
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

function sectionByCentre() {
  if (!BYCENTRE.anchor_date) {
    return `
      <section id="bycentre">
        <div class="eyebrow">By Yulu Centre</div>
        <div class="title disp" style="font-size:28px;">Fulfillment by Individual Centre</div>
        <div class="panel"><div class="empty-note">No centre-level data captured yet — this fills in once Daily_Ops_Metrics_ByCentre's first run lands.</div></div>
      </section>`;
  }

  const tabs = CENTRE_METRIC_TABS.map((t, i) =>
    `<button class="tab ${i === ACTIVE_CENTRE_TAB ? "on" : ""}" onclick="setCentreTab(${i})">${t.label}</button>`
  ).join("");

  const metric = CENTRE_METRIC_TABS[ACTIVE_CENTRE_TAB];
  const rows = [BLR_TOTAL_LABEL, ...centreNames()];

  const body = rows.map(centre => {
    const cells = PERIOD_KEYS.map((pk, i) => {
      const v = getCentre(centre, metric.key, pk);
      if (v === null || v === undefined) return `<td class="dash">–</td>`;
      let deltaHtml = "";
      if (i > 0) {
        const prev = getCentre(centre, metric.key, PERIOD_KEYS[i - 1]);
        if (prev !== null && prev !== undefined && prev !== 0) {
          const pct = ((v - prev) / Math.abs(prev)) * 100;
          const dir = pct >= 0 ? "up" : "down";
          const arrow = pct >= 0 ? "▲" : "▼";
          deltaHtml = `<span class="delta ${dir}">${arrow}${Math.abs(pct).toFixed(0)}%</span>`;
        }
      }
      return `<td class="num">${metric.fmt(v)}${metric.unit}${deltaHtml}</td>`;
    }).join("");
    return `<tr class="${centre === BLR_TOTAL_LABEL ? "total" : ""}"><td>${centre}</td>${cells}</tr>`;
  }).join("");

  return `
    <section id="bycentre">
      <div class="eyebrow">By Yulu Centre</div>
      <div class="title disp" style="font-size:28px;">Fulfillment by Individual Centre</div>
      <p class="deck-desc">Same fulfillment% definitions as the cluster-level Period Comparison, broken down by individual Yulu Centre instead — each column compared against the one just before it in time.</p>
      <div class="tabs">${tabs}</div>
      <div class="panel" style="overflow-x:auto;">
        <table class="ptable">
          <thead><tr><th>Yulu Centre</th>${PERIOD_KEYS.map(k => `<th>${PERIOD_LABELS[k]}</th>`).join("")}</tr></thead>
          <tbody>${body}</tbody>
        </table>
      </div>
    </section>`;
}

function sectionClusters() {
  const allDates = DATA.dates || [];
  // Only offer dates where at least one cluster actually has a DAU value
  // -- otherwise the picker would list 60 trend-series days even though
  // Daily_Ops_Metrics itself has far fewer real rows captured so far.
  const availableDates = allDates.filter(d =>
    clusterNames().some(c => getForRange(c, "dau", d, d) !== null) || getForRange(BLR_TOTAL_LABEL, "dau", d, d) !== null
  );
  const defaultDate = availableDates.length ? availableDates[availableDates.length - 1] : DATA.anchor_date;
  if (!ACTIVE_CLUSTERS_START || !availableDates.includes(ACTIVE_CLUSTERS_START)) ACTIVE_CLUSTERS_START = defaultDate;
  if (!ACTIVE_CLUSTERS_END || !availableDates.includes(ACTIVE_CLUSTERS_END)) ACTIVE_CLUSTERS_END = defaultDate;
  // Swap rather than reject if the user picks "From" after "To" -- a
  // stray "invalid range" error is worse than just showing it flipped.
  let startDate = ACTIVE_CLUSTERS_START, endDate = ACTIVE_CLUSTERS_END;
  if (startDate > endDate) { [startDate, endDate] = [endDate, startDate]; }

  const minDate = availableDates.length ? availableDates[0] : "";
  const maxDate = availableDates.length ? availableDates[availableDates.length - 1] : "";
  const isRange = startDate !== endDate;
  const rangeLabel = isRange ? `${fmtDate(startDate)} – ${fmtDate(endDate)}` : fmtDate(startDate);

  // Re-sort by the SELECTED range's average DAU (not always "latest"),
  // so the ranking on screen actually matches whatever's being viewed.
  const namesForRange = clusterNames().slice().sort(
    (a, b) => (getForRange(b, "dau", startDate, endDate) || 0) - (getForRange(a, "dau", startDate, endDate) || 0)
  );

  const dauRows  = namesForRange.map(c => ({ name: c, dau: getForRange(c, "dau", startDate, endDate) }));
  const prodRows = namesForRange.map(c => ({ name: c, v: getForRange(c, "mechanic_productivity_90d", startDate, endDate) }));
  const enqRows  = namesForRange.map(c => ({ name: c, v: getForRange(c, "enquiry_total", startDate, endDate) }));
  const convRows = namesForRange.map(c => ({ name: c, v: getForRange(c, "enquiry_to_attachment_pct", startDate, endDate) }));
  const cityProd = getForRange(BLR_TOTAL_LABEL, "mechanic_productivity_90d", startDate, endDate);

  return `
    <section id="clusters">
      <div class="eyebrow">By Cluster</div>
      <div class="title disp" style="font-size:28px;">Ranked by Cluster</div>
      <p class="deck-desc">Where the fleet is actually being used, and how maintenance/enquiries break down${isRange ? ", averaged per day" : ""} for ${rangeLabel}.</p>
      <div class="toolbar">
        <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
          <label style="font-size:12.5px;color:var(--text-dim);display:flex;align-items:center;gap:6px;">From
            <input type="date" class="cluster-pick" value="${startDate}" min="${minDate}" max="${maxDate}" onchange="setClustersStart(this.value)">
          </label>
          <label style="font-size:12.5px;color:var(--text-dim);display:flex;align-items:center;gap:6px;">To
            <input type="date" class="cluster-pick" value="${endDate}" min="${minDate}" max="${maxDate}" onchange="setClustersEnd(this.value)">
          </label>
        </div>
      </div>
      <div class="panel">
        <h3>Daily Active Users${isRange ? " (avg/day)" : ""}</h3>
        ${barChart(dauRows, r => r.dau, { fmt: v => Math.round(v).toLocaleString("en-IN") })}
      </div>
      <div class="panel">
        <h3>Mechanic Productivity — city avg. ${cityProd !== null ? cityProd.toFixed(2) : "—"}</h3>
        ${barChart(prodRows, r => r.v, { fmt: v => v.toFixed(2) })}
      </div>
      <div class="panel">
        <h3>Enquiries logged &amp; conversion to Attachment${isRange ? " (avg/day)" : ""}</h3>
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
    sectionByCentre(),
    sectionAttention(),
  ].join("");
  renderCharts();
}

function setTab(i) { ACTIVE_TAB = i; render(); }
function setCentreTab(i) { ACTIVE_CENTRE_TAB = i; render(); }
function setCluster(c) { ACTIVE_CLUSTER = c; render(); }
function setClustersStart(d) { ACTIVE_CLUSTERS_START = d; render(); }
function setClustersEnd(d) { ACTIVE_CLUSTERS_END = d; render(); }

document.getElementById("asOfLabel").textContent = fmtDate(DATA.anchor_date);
render();

// Auto-refresh: a tab left open on this static page will otherwise never
// see a newer deploy on its own. Reload periodically so the next build
// (triggered right after every ETL run — see deploy-dashboard.yml) shows
// up without anyone manually hitting refresh.
setTimeout(() => location.reload(), AUTO_REFRESH_MS);
