/* fake-tickets UI — dependency-free. All server data is rendered with textContent (no innerHTML). */
"use strict";

const $ = (sel, root = document) => root.querySelector(sel);
const state = {
  me: null, workloads: [], connections: [], connId: null, wl: null,
  runs: [], selRun: null, runDetail: null, series: [], since: null, timers: {},
};

/* ---------- helpers ---------- */
function h(tag, attrs = {}, ...kids) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k === "class") el.className = v;
    else if (k === "text") el.textContent = v;
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else if (k in el && typeof v !== "string") el[k] = v;
    else el.setAttribute(k, v === true ? "" : v);
  }
  for (const kid of kids.flat()) if (kid !== null && kid !== undefined && kid !== false) el.append(kid.nodeType ? kid : String(kid));
  return el;
}
async function api(path, opts = {}) {
  const headers = { "X-Requested-With": "fake-tickets", ...(opts.headers || {}) };
  let body = opts.body;
  if (body && !(body instanceof FormData)) { headers["Content-Type"] = "application/json"; body = JSON.stringify(body); }
  const r = await fetch(path, { ...opts, headers, body, credentials: "same-origin" });
  if (r.status === 401 && path !== "/api/login" && state.me && state.me.auth_mode !== "none") { showLogin(); throw new Error("login required"); }
  if (r.status === 204) return null;
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail || data));
  return data;
}
function toast(msg) {
  const t = $("#toast"); t.textContent = msg; t.classList.add("show");
  clearTimeout(state.timers.toast); state.timers.toast = setTimeout(() => t.classList.remove("show"), 3500);
}
const fmtInt = (n) => (n === null || n === undefined) ? "–" : Number(n).toLocaleString();
function fmtRate(n) {
  if (!n) return "0";
  if (n >= 1e6) return (n / 1e6).toFixed(n >= 1e7 ? 0 : 1) + "M";
  if (n >= 1e4) return (n / 1e3).toFixed(0) + "k";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "k";
  return n >= 10 ? Math.round(n).toString() : n.toFixed(1);
}
const fmtTime = (t) => t ? new Date(t * 1000).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "–";
function fmtDur(s) {
  if (s === null || s === undefined) return "–";
  s = Math.max(0, Math.round(s));
  const d = Math.floor(s / 86400), hh = Math.floor(s % 86400 / 3600), m = Math.floor(s % 3600 / 60), ss = s % 60;
  return d ? `${d}d ${hh}h` : hh ? `${hh}h ${m}m` : m ? `${m}m ${ss}s` : `${ss}s`;
}
function fmtBytes(b) {
  const u = ["B", "KB", "MB", "GB", "TB"]; let i = 0;
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return `${b.toFixed(b >= 10 || i === 0 ? 0 : 1)} ${u[i]}`;
}

/* ---------- boot / auth ---------- */
async function boot() {
  state.me = await api("/api/me");
  $("#version").textContent = "v" + state.me.version;
  if (state.me.auth_mode !== "none" && !state.me.user) return showLogin();
  showApp();
}
function showLogin() {
  $("#app-view").classList.add("hidden"); $("#login-view").classList.remove("hidden");
  $("#logout").classList.add("hidden"); $("#whoami").textContent = "";
  $("#login-hint").textContent = state.me && state.me.auth_mode === "ldap" ? "Use your homelab directory (LDAP) account." : "Use your local account.";
  Object.values(state.timers).forEach(clearInterval);
}
async function showApp() {
  $("#login-view").classList.add("hidden"); $("#app-view").classList.remove("hidden");
  $("#whoami").textContent = state.me.user || "";
  $("#logout").classList.toggle("hidden", state.me.auth_mode === "none");
  await Promise.all([loadWorkloads(), loadConnections()]);
  refreshRuns();
  state.timers.runs = setInterval(refreshRuns, 2000);
  const m = location.hash.match(/run=([a-z0-9]+)/i);
  if (m) selectRun(m[1]);
}
$("#login-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const f = new FormData(e.target);
  $("#login-error").textContent = "";
  try {
    const r = await api("/api/login", { method: "POST", body: { username: f.get("username"), password: f.get("password") } });
    state.me.user = r.user; e.target.reset(); showApp();
  } catch (err) { $("#login-error").textContent = err.message; }
});
$("#logout").addEventListener("click", async () => { await api("/api/logout", { method: "POST" }); state.me.user = null; showLogin(); });

/* ---------- connections ---------- */
async function loadConnections() {
  state.connections = await api("/api/connections");
  if (!state.connections.find((c) => c.id === state.connId)) state.connId = state.connections[0] ? state.connections[0].id : null;
  renderConnections();
}
function connMeta(c) {
  if (c.kind === "astra") return `${c.scb_filename || "secure connect bundle"} · ks ${c.keyspace}`;
  return `${(c.contact_points || []).join(", ")}:${c.port} · ks ${c.keyspace}${c.tls_enabled ? " · TLS" : ""}`;
}
function renderConnections() {
  const list = $("#conn-list"); list.replaceChildren();
  if (!state.connections.length) list.append(h("p", { class: "muted small", text: "No connection yet. Create one to get started." }));
  for (const c of state.connections) {
    list.append(h("label", { class: "conn" },
      h("input", { type: "radio", name: "conn", value: c.id, checked: c.id === state.connId, onchange: () => { state.connId = c.id; updateButtons(); } }),
      h("div", {}, h("div", { class: "name", text: c.name }), h("div", { class: "meta", text: connMeta(c), title: connMeta(c) })),
      h("span", { class: `badge ${c.kind}`, text: c.kind })));
  }
  updateButtons();
}
function updateButtons() {
  const has = state.connId !== null;
  for (const id of ["#test-conn", "#edit-conn", "#del-conn"]) $(id).disabled = !has;
  $("#start-run").disabled = !(has && state.wl);
}
$("#new-conn").addEventListener("click", () => openConnDialog(null));
$("#edit-conn").addEventListener("click", () => openConnDialog(state.connections.find((c) => c.id === state.connId)));
$("#del-conn").addEventListener("click", async () => {
  const c = state.connections.find((x) => x.id === state.connId);
  if (!c || !confirm(`Delete connection "${c.name}"? Stored secrets are removed.`)) return;
  await api(`/api/connections/${c.id}`, { method: "DELETE" }); toast("Connection deleted"); loadConnections();
});
$("#test-conn").addEventListener("click", async () => {
  try {
    const run = await api("/api/runs", { method: "POST", body: { connection_id: state.connId, workload: "connectivity", params: {} } });
    toast("Connectivity test started"); selectRun(run.id); refreshRuns();
  } catch (err) { toast(err.message); }
});

const dlg = $("#conn-dialog"), cform = $("#conn-form");
function syncKind() {
  const kind = cform.kind.value;
  cform.querySelector(".kind-hosts").classList.toggle("hidden", kind === "astra");
  cform.querySelector(".kind-astra").classList.toggle("hidden", kind !== "astra");
  cform.querySelector(".tls").classList.toggle("hidden", !cform.tls_enabled.checked);
}
cform.addEventListener("change", syncKind);
function openConnDialog(c) {
  cform.reset(); $("#conn-error").textContent = "";
  cform.dataset.id = c ? c.id : "";
  $("#conn-title").textContent = c ? `Edit ${c.name}` : "New connection";
  if (c) {
    for (const [k, v] of Object.entries(c)) {
      const el = cform.elements[k];
      if (!el || v === null || v === undefined || el.type === "file") continue;
      if (el instanceof RadioNodeList) el.value = v;
      else if (el.type === "checkbox") el.checked = !!v;
      else el.value = Array.isArray(v) ? v.join(", ") : v;
    }
    for (const k of ["password", "tls_key_pem", "astra_token", "astra_client_secret"]) {
      cform.elements[k].placeholder = (c.has_secret || {})[k] ? "•••••• (unchanged)" : "";
    }
  }
  $("#scb-current").textContent = c && c.scb_filename ? `Current bundle: ${c.scb_filename} (upload a new one to replace)` : "";
  syncKind(); dlg.showModal();
}
$("#conn-cancel").addEventListener("click", () => dlg.close());
cform.addEventListener("submit", async (e) => {
  e.preventDefault();
  const f = cform.elements, kind = cform.kind.value;
  const data = { name: f.name.value, kind, keyspace: f.keyspace.value.trim() };
  if (f.protocol_version.value) data.protocol_version = Number(f.protocol_version.value);
  if (kind === "astra") {
    for (const k of ["astra_token", "astra_client_id", "astra_client_secret"]) if (f[k].value) data[k] = f[k].value;
  } else {
    Object.assign(data, {
      contact_points: f.contact_points.value, port: Number(f.port.value || 9042),
      local_dc: f.local_dc.value.trim() || null, replication_factor: Number(f.replication_factor.value || 3),
      username: f.username.value.trim() || null, tls_enabled: f.tls_enabled.checked,
      tls_verify_hostname: f.tls_verify_hostname.checked, tls_ca_pem: f.tls_ca_pem.value.trim() || null,
      tls_cert_pem: f.tls_cert_pem.value.trim() || null,
    });
    if (f.password.value) data.password = f.password.value;
    if (f.tls_key_pem.value.trim()) data.tls_key_pem = f.tls_key_pem.value.trim();
  }
  const fd = new FormData(); fd.append("data", JSON.stringify(data));
  if (kind === "astra" && f.scb.files[0]) fd.append("scb", f.scb.files[0]);
  const id = cform.dataset.id;
  try {
    const saved = await api(id ? `/api/connections/${id}` : "/api/connections", { method: id ? "PUT" : "POST", body: fd });
    dlg.close(); state.connId = saved.id; toast("Connection saved"); loadConnections();
  } catch (err) { $("#conn-error").textContent = err.message; }
});

/* ---------- workloads & dynamic form ---------- */
async function loadWorkloads() {
  state.workloads = await api("/api/workloads");
  const list = $("#wl-list"); list.replaceChildren();
  const cats = [...new Set(state.workloads.map((w) => w.category || "Data"))];
  const order = ["Data", "Compaction", "Bug repro"];
  cats.sort((a, b) => (order.indexOf(a) + 1 || 99) - (order.indexOf(b) + 1 || 99));
  for (const cat of cats) {
    list.append(h("div", { class: "wl-cat", text: cat }));
    for (const w of state.workloads.filter((x) => (x.category || "Data") === cat)) list.append(wlCard(w));
  }
  const first = state.workloads.find((w) => w.name === "write") || state.workloads[0];
  if (first) { list.querySelector(`input[value="${first.name}"]`).checked = true; selectWorkload(first.name); }
}
function wlCard(w) {
  return h("label", { class: "wl" },
    h("input", { type: "radio", name: "wl", value: w.name, onchange: () => selectWorkload(w.name) }),
    h("span", { class: "t" }, w.title, h("span", { class: "badge", text: w.kind === "oneshot" ? "one-shot" : "continuous" })),
    h("span", { class: "d", text: w.description }));
}
function visible(p, values) {
  if (!p.visible_if) return true;
  return Object.entries(p.visible_if).every(([k, allowed]) => allowed.some((a) =>
    (a && typeof a === "object" && "gt" in a) ? Number(values[k]) > a.gt : String(values[k]) === String(a)));
}
function formValues() {
  const out = {};
  for (const el of $("#wl-form").querySelectorAll("[data-param]")) {
    out[el.dataset.param] = el.type === "checkbox" ? el.checked : el.value;
  }
  return out;
}
function paramField(p) {
  const id = `p-${p.name}`;
  let input;
  if (p.type === "choice") {
    input = h("select", { id, "data-param": p.name }, p.choices.map((c) => h("option", { value: c, text: c, selected: c === p.default })));
  } else if (p.type === "bool") {
    return h("label", { class: "inline", "data-wrap": p.name, title: p.help || "" },
      h("input", { id, type: "checkbox", "data-param": p.name, checked: !!p.default }), p.label || p.name);
  } else {
    input = h("input", { id, "data-param": p.name, value: p.default, type: (p.type === "int" || p.type === "float") ? "number" : "text",
      step: p.type === "float" ? "any" : "1", min: p.min ?? null, max: p.max ?? null });
  }
  return h("label", { "data-wrap": p.name }, p.label || p.name, input, p.help ? h("span", { class: "help", text: p.help }) : null);
}
function selectWorkload(name) {
  const w = state.workloads.find((x) => x.name === name); state.wl = w;
  const by = (g) => w.params.filter((p) => p.group === g);
  $("#wl-params").replaceChildren(...by("workload").map(paramField));
  const cmp = by("compaction");
  $("#wl-cmp").replaceChildren(...cmp.map(paramField));
  $("#cmp-box").classList.toggle("hidden", !cmp.length);
  const stop = by("stop");
  state.amountUpdate = null;
  $("#wl-stop").replaceChildren(stop.length ? amountWidget(w) : "");
  $("#amount-box").classList.toggle("hidden", !stop.length);
  const adv = by("advanced");
  $("#wl-adv").replaceChildren(...adv.map(paramField));
  $("#wl-adv-wrap").classList.toggle("hidden", !adv.length);
  const rateP = w.params.find((p) => p.name === "rate");
  $("#rate-box").classList.toggle("hidden", !rateP);
  if (rateP) {
    $("#rate-control").replaceChildren(rateWidget(rateP.default, (v) => { $("#wl-form").dataset.rate = v; suggest(v); if (state.amountUpdate) state.amountUpdate(); }));
    $("#wl-form").dataset.rate = rateP.default;
    $("#rate-extra").replaceChildren(...by("rate").filter((p) => p.name !== "rate").map(paramField));
    suggest(rateP.default);
  }
  applyVisibility(); prefillCursor(); updateButtons();
  if (state.amountUpdate) state.amountUpdate();
}
function applyVisibility() {
  if (!state.wl) return;
  const vals = formValues();
  for (const p of state.wl.params) {
    const wrap = $("#wl-form").querySelector(`[data-wrap="${p.name}"]`);
    if (wrap) wrap.classList.toggle("hidden", !visible(p, vals));
  }
}
$("#wl-form").addEventListener("change", (e) => {
  applyVisibility();
  if (state.amountUpdate) state.amountUpdate();
  if (e.target.dataset.param === "table" || e.target.dataset.param === "keyspace") prefillCursor();
});
async function prefillCursor() {
  const w = state.wl;
  const startEl = $("#p-start_id"), maxEl = $("#p-id_max");
  const target = startEl || maxEl;
  if (!w || !target || state.connId === null) return;
  const v = formValues();
  try {
    const r = await api(`/api/cursor?connection_id=${state.connId}&table=${encodeURIComponent(v.table || "")}&keyspace=${encodeURIComponent(v.keyspace || "")}`);
    const help = target.parentElement.querySelector(".help");
    if (help) help.textContent = r.next_id !== null ? `-1 = auto → ${startEl ? "resume at" : "up to"} ${fmtInt(startEl ? r.next_id : r.next_id - 1)} (from previous runs)` : "-1 = auto (no previous run on this table → " + (startEl ? "start at 0)" : "probe the table)");
  } catch (_) { /* cursor is a convenience */ }
}
$("#conn-list").addEventListener("change", prefillCursor);
function suggest(rate) {
  const p = $("#p-processes");
  const hint = $("#rate-hint");
  const need = rate > 0 ? Math.ceil(rate / 6000) : 0;
  if (hint) hint.textContent = (p && need > Number(p.value)) ? `Tip: one worker process sustains ~5–8k ops/s in Python — consider ${need} processes.` : "";
}

/* "How much": until stopped / N records / duration, with presets and live estimates */
function parseCount(s) {
  const m = String(s).trim().toLowerCase().replace(/[,_\s]/g, "").match(/^(\d+(?:\.\d+)?)([kmb]?)$/);
  return m ? Math.round(Number(m[1]) * { "": 1, k: 1e3, m: 1e6, b: 1e9 }[m[2]]) : NaN;
}
function parseDuration(s) {  // "90s", "10m", "1.5h", "2d"; a bare number means minutes
  const m = String(s).trim().toLowerCase().replace(/\s/g, "").match(/^(\d+(?:\.\d+)?)([smhd]?)$/);
  return m ? Math.round(Number(m[1]) * { "": 60, s: 1, m: 60, h: 3600, d: 86400 }[m[2]]) : NaN;
}
function fmtCount(n) {
  if (n >= 1e9) return +(n / 1e9).toFixed(2) + "B";
  if (n >= 1e6) return +(n / 1e6).toFixed(2) + "M";
  if (n >= 1e3) return +(n / 1e3).toFixed(2) + "k";
  return String(n);
}
const countNoun = (w) => { const p = (w.params || []).find((x) => x.name === "count"); return p ? p.label.replace(/^number of /i, "") : "operations"; };
function writeShare(w, v) {  // fraction of ops that count towards "count"
  let share = v.read_pct !== undefined ? 1 - Number(v.read_pct || 0) / 100 : 1;
  if (w.name === "tombstones" && ["row", "cell"].includes(v.delete_kind)) share /= 2;  // one delete per write
  return Math.max(share, 0.0001);
}
function amountWidget(w) {
  const hasCount = w.params.some((p) => p.name === "count"), hasDur = w.params.some((p) => p.name === "duration_s");
  const noun = countNoun(w);
  const hidC = h("input", { type: "hidden", "data-param": "count", value: "0" });
  const hidD = h("input", { type: "hidden", "data-param": "duration_s", value: "0" });
  let mode = w.category === "Bug repro" || !hasCount ? "forever" : "records";
  const modes = [["forever", "Until stopped"], hasCount && ["records", `Number of ${noun}`], hasDur && ["duration", "Duration"]].filter(Boolean);
  const seg = h("div", { class: "seg", role: "radiogroup", "aria-label": "How much" }, modes.map(([m, label]) =>
    h("label", {}, h("input", { type: "radio", name: "amount-mode", value: m, checked: m === mode, onchange: () => { mode = m; sync(); } }), label)));
  const rec = h("input", { value: "100k", "aria-label": `Number of ${noun}`, inputmode: "decimal", oninput: () => sync() });
  const dur = h("input", { value: "10m", "aria-label": "Duration", oninput: () => sync() });
  const recPills = [1e3, 1e4, 1e5, 1e6, 1e7, 1e8].map((n) => h("button", { type: "button", class: "pill", text: fmtCount(n), onclick: () => { rec.value = fmtCount(n); sync(); } }));
  const durPills = [["1 min", "1m"], ["10 min", "10m"], ["1 h", "1h"], ["8 h", "8h"], ["24 h", "24h"], ["7 days", "7d"]].map(([l, v]) => h("button", { type: "button", class: "pill", text: l, onclick: () => { dur.value = v; sync(); } }));
  const recRow = h("div", { class: "amount-row" }, rec, h("div", { class: "presets" }, recPills));
  const durRow = h("div", { class: "amount-row" }, dur, h("div", { class: "presets" }, durPills));
  const est = h("div", { class: "amount-est", "aria-live": "polite" });
  function sync() {
    const n = parseCount(rec.value), secs = parseDuration(dur.value);
    recRow.classList.toggle("hidden", mode !== "records"); durRow.classList.toggle("hidden", mode !== "duration");
    rec.setCustomValidity(mode === "records" && !(n > 0) ? "e.g. 250000, 250k, 1.5M" : "");
    dur.setCustomValidity(mode === "duration" && !(secs > 0) ? "e.g. 90s, 10m, 1.5h, 2d" : "");
    hidC.value = mode === "records" && n > 0 ? String(n) : "0";
    hidD.value = mode === "duration" && secs > 0 ? String(secs) : "0";
    recPills.forEach((p) => p.classList.toggle("active", parseCount(p.textContent) === n));
    durPills.forEach((p, i) => p.classList.toggle("active", parseDuration(["1m", "10m", "1h", "8h", "24h", "7d"][i]) === secs));
    const v = formValues(), rate = Number($("#wl-form").dataset.rate || 0);
    const perSec = rate * writeShare(w, v);
    const rowBytes = w.approx_row_bytes ? w.approx_row_bytes + Number(v.payload_bytes || v.blob_bytes || 0) : 0;
    const size = (k) => rowBytes ? ` · ≈ ${fmtBytes(k * rowBytes)} raw (before RF / compression)` : "";
    if (mode === "records") {
      est.textContent = !(n > 0) ? "Enter a number such as 250000, 250k or 1.5M." :
        rate > 0 ? `${fmtInt(n)} ${noun} ≈ ${fmtDur(n / perSec)} at this rate${size(n)}` : `${fmtInt(n)} ${noun} — time depends on the cluster (unlimited rate)${size(n)}`;
    } else if (mode === "duration") {
      est.textContent = !(secs > 0) ? "Enter a duration such as 90s, 10m, 1.5h or 2d." :
        rate > 0 ? `${fmtDur(secs)} ≈ ${fmtInt(Math.round(perSec * secs))} ${noun}${size(perSec * secs)}` : `${fmtDur(secs)} at unlimited rate`;
    } else {
      est.textContent = `Runs until you press Stop${rate > 0 ? ` · ≈ ${fmtInt(Math.round(perSec * 3600))} ${noun}/h${size(perSec * 3600)}/h` : ""}`;
    }
  }
  state.amountUpdate = sync;
  return h("div", { class: "amount" }, seg, recRow, durRow, est, hidC, hidD);
}

/* Rate widget: log slider 1 → 100k ops/s + unlimited, presets, live estimate */
const RATE_MAX_EXP = 5; // 10^5
const posToRate = (pos) => { const r = Math.pow(10, pos / 1000 * RATE_MAX_EXP); return r < 10 ? Math.round(r * 10) / 10 : r < 100 ? Math.round(r) : r < 1000 ? Math.round(r / 5) * 5 : Math.round(r / 50) * 50; };
const rateToPos = (r) => Math.round(Math.log10(Math.max(1, r)) / RATE_MAX_EXP * 1000);
function rateWidget(initial, onChange, opts = {}) {
  let rate = Number(initial);
  const val = h("span", { class: "rate-value" }), sub = h("span", { class: "rate-sub" });
  const slider = h("input", { type: "range", min: 0, max: 1000, step: 1, "aria-label": "Target rate" });
  const num = h("input", { type: "number", min: 0, step: "any", "aria-label": "Target ops per second" });
  const presets = [["Trickle", 10], ["Slow & steady", 100], ["1k/s", 1000], ["5k/s", 5000], ["20k/s", 20000], ["Unlimited", 0]];
  const pills = presets.map(([label, r]) => h("button", { type: "button", class: "pill", text: label, onclick: () => set(r, true) }));
  function render() {
    val.textContent = rate === 0 ? "Unlimited" : `${fmtRate(rate)} ops/s`;
    const perHour = rate * 3600, bytes = perHour * 110;
    sub.textContent = rate === 0 ? "bounded only by concurrency × latency" : `≈ ${fmtInt(Math.round(perHour))} rows/h · ≈ ${fmtBytes(bytes)}/h raw · ${fmtInt(Math.round(rate * 86400))}/day`;
    slider.value = rate === 0 ? 1000 : rateToPos(rate); slider.disabled = rate === 0;
    num.value = rate;
    pills.forEach((p, i) => p.classList.toggle("active", presets[i][1] === rate));
  }
  function set(r, commit) { rate = Math.max(0, Number(r) || 0); render(); if (commit || !opts.commitOnRelease) onChange(rate); }
  slider.addEventListener("input", () => set(posToRate(Number(slider.value)), false));
  slider.addEventListener("change", () => onChange(rate));
  num.addEventListener("change", () => set(num.value, true));
  render();
  return h("div", { class: "rate-control" },
    h("div", { class: "rate-top" }, val, sub),
    h("div", { class: "rate-row" }, slider, num),
    h("div", { class: "presets" }, pills),
    opts.live ? null : h("div", { class: "hint", id: "rate-hint" }));
}

$("#wl-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  $("#start-error").textContent = "";
  const w = state.wl, vals = formValues(), params = {};
  for (const p of w.params) {
    if (p.name === "rate") { params.rate = Number($("#wl-form").dataset.rate); continue; }
    if (!(p.name in vals) || !visible(p, vals)) continue;
    params[p.name] = vals[p.name];
  }
  try {
    const run = await api("/api/runs", { method: "POST", body: { connection_id: state.connId, workload: w.name, params, label: $("#run-label").value || null } });
    toast(`Run ${run.id} started`); selectRun(run.id); refreshRuns();
  } catch (err) { $("#start-error").textContent = err.message; }
});

/* ---------- runs ---------- */
$("#only-active").addEventListener("change", renderRuns);
async function refreshRuns() {
  try {
    state.runs = await api("/api/runs");
    $("#backend-status").className = "dot up"; $("#backend-status").title = "backend reachable";
  } catch (err) {
    $("#backend-status").className = "dot down"; $("#backend-status").title = err.message; return;
  }
  renderRuns();
}
const ACTIVE = ["starting", "running", "stopping"];
function renderRuns() {
  const only = $("#only-active").checked;
  const rows = state.runs.filter((r) => !only || ACTIVE.includes(r.state));
  $("#runs-empty").classList.toggle("hidden", rows.length > 0);
  $("#runs-table tbody").replaceChildren(...rows.map((r) => h("tr", { class: r.id === state.selRun ? "sel" : "", onclick: () => selectRun(r.id) },
    h("td", { class: "mono", text: r.id, title: r.label || "" }),
    h("td", { text: r.workload }),
    h("td", { text: r.target || "–", title: r.connection_name || "" }),
    h("td", {}, h("span", { class: `state ${r.state}`, text: r.state })),
    h("td", { class: "num", text: ACTIVE.includes(r.state) ? fmtRate(r.rate) : "–" }),
    h("td", { class: "num", text: targetText(r) }),
    h("td", {}, progressCell(r)),
    h("td", { class: "num", text: fmtInt(r.total_ok) }),
    h("td", { class: "num", text: fmtInt(r.total_err) }),
    h("td", { class: "num", text: r.latency_ms ? r.latency_ms.p99 : "–" }),
    h("td", { text: r.owner || "–" }),
    h("td", { text: fmtTime(r.started_at || r.created_at) }))));
}
function progressOf(d) {
  if (d.count_target) return { frac: Math.min(1, (d.counted || 0) / d.count_target), kind: "count" };
  if (d.duration_target && d.started_at) return { frac: Math.min(1, ((d.ended_at || Date.now() / 1000) - d.started_at) / d.duration_target), kind: "duration" };
  return null;
}
function bar(frac) {
  const fill = h("span");
  fill.style.width = `${(frac * 100).toFixed(1)}%`;  // CSSOM, not a style attribute: allowed by the strict CSP
  return h("div", { class: "progress", role: "progressbar", "aria-valuemin": 0, "aria-valuemax": 100, "aria-valuenow": Math.round(frac * 100) }, fill);
}
function progressCell(r) {
  const p = progressOf(r);
  if (!p) return ACTIVE.includes(r.state) ? h("span", { class: "muted", text: "∞" }) : "–";
  const frac = r.reason === "count_reached" || r.reason === "duration_reached" ? 1 : p.frac;
  return h("span", {}, bar(frac), `${Math.floor(frac * 100)}%`);
}
function targetText(r) {
  const t = r.rate_target ?? (r.params || {}).rate;
  if (t === undefined || t === null || (r.params && r.params.rate === undefined)) return "–";
  return t === 0 ? "∞" : fmtRate(t);
}
function selectRun(id) {
  history.replaceState(null, "", `#run=${id}`);
  state.selRun = id; state.countHist = []; state.series = []; state.since = null; state.runDetail = null; $("#run-detail").dataset.run = "";
  clearInterval(state.timers.detail);
  $("#run-detail").classList.remove("hidden");
  pollRun(); state.timers.detail = setInterval(pollRun, 1000);
  renderRuns();
}
async function pollRun() {
  const id = state.selRun; if (!id) return;
  let d;
  try { d = await api(`/api/runs/${id}${state.since ? `?since=${state.since}` : ""}`); } catch (err) { return; }
  if (id !== state.selRun) return;
  if (d.series && d.series.length) { state.series.push(...d.series); state.since = d.series[d.series.length - 1].t; if (state.series.length > 3600) state.series.splice(0, state.series.length - 3600); }
  state.runDetail = d;
  const ch = state.countHist || (state.countHist = []);
  if (ch.length && ch[0].id !== id) ch.length = 0;
  ch.push({ id, t: Date.now() / 1000, n: d.counted || 0 }); if (ch.length > 15) ch.shift();
  renderDetail(d);
  if (!ACTIVE.includes(d.state)) clearInterval(state.timers.detail);
}
function tile(k, v, small) { return h("div", { class: "tile" }, h("div", { class: "k", text: k }), h("div", { class: "v" }, v, small ? h("small", { text: " " + small }) : null)); }
function renderDetail(d) {
  const box = $("#run-detail");
  const active = ACTIVE.includes(d.state);
  const wl = state.workloads.find((w) => w.name === d.workload) || { params: [], kind: "continuous" };
  const elapsed = d.started_at ? ((d.ended_at || Date.now() / 1000) - d.started_at) : null;
  const lat = d.latency_ms || {};
  const head = h("div", { class: "detail-head" },
    h("div", {},
      h("h2", {}, `${wl.title || d.workload} `, h("span", { class: "mono muted", text: d.id }), " ", h("span", { class: `state ${d.state}`, text: d.state })),
      h("div", { class: "sub", text: [d.target, d.connection_name, d.owner && `by ${d.owner}`, d.label, d.reason && `(${d.reason})`].filter(Boolean).join(" · ") })),
    h("div", { class: "row gap" },
      active ? h("button", { class: "danger", type: "button", text: "Stop", onclick: async () => { await api(`/api/runs/${d.id}/stop`, { method: "POST" }); toast("Stopping…"); } }) : null,
      !active ? h("button", { class: "ghost", type: "button", text: "Delete", onclick: async () => { await api(`/api/runs/${d.id}`, { method: "DELETE" }); state.selRun = null; box.classList.add("hidden"); refreshRuns(); } }) : null,
      h("button", { class: "ghost", type: "button", text: "Close", onclick: () => { history.replaceState(null, "", location.pathname); state.selRun = null; clearInterval(state.timers.detail); box.classList.add("hidden"); renderRuns(); } })));
  const counters = d.counters || {};
  const tiles = h("div", { class: "tiles" },
    tile("Rate now", active ? fmtRate(d.rate) : "–", "ops/s"),
    wl.kind !== "oneshot" ? tile("Target", targetText(d), "ops/s") : null,
    tile("Completed", fmtInt(d.total_ok)),
    tile("Errors", fmtInt(d.total_err)),
    tile("p50 / p99", `${lat.p50 ?? "–"} / ${lat.p99 ?? "–"}`, "ms"),
    tile("Elapsed", fmtDur(elapsed)),
    d.next_id !== null && d.next_id !== undefined ? tile("Next id", fmtInt(d.next_id)) : null,
    counters.hit !== undefined || counters.miss !== undefined ? tile("Hit / miss", `${fmtInt(counters.hit || 0)} / ${fmtInt(counters.miss || 0)}`) : null,
    d.progress && d.progress.overlaps ? tile("Overlaps", fmtInt(d.progress.overlaps)) : null,
    d.progress && d.progress.backdated ? tile("Back-dated", fmtInt(d.progress.backdated)) : null,
    d.progress && d.progress.pass ? tile("Pass over keys", fmtInt(d.progress.pass)) : null,
    d.progress && d.progress.deletes ? tile("Deletes", fmtInt(d.progress.deletes)) : null,
    d.progress && d.progress.late ? tile("Late writes", fmtInt(d.progress.late)) : null);
  const prog = progressOf(d);
  let progBox = null;
  if (prog) {
    const done = d.reason === "count_reached" || d.reason === "duration_reached";
    const frac = done ? 1 : prog.frac;
    const ch = state.countHist || [];
    let eta = "";
    if (active && prog.kind === "count" && ch.length > 2) {
      const r = (ch[ch.length - 1].n - ch[0].n) / Math.max(0.001, ch[ch.length - 1].t - ch[0].t);
      if (r > 0) eta = `ETA ${fmtDur((d.count_target - d.counted) / r)}`;
    } else if (active && prog.kind === "duration") eta = `${fmtDur(d.duration_target * (1 - frac))} left`;
    const label = prog.kind === "count" ? `${fmtInt(done ? Math.max(d.counted || 0, d.count_target) : d.counted)} / ${fmtInt(d.count_target)} ${countNoun(wl)}`
      : `${fmtDur(frac * d.duration_target)} / ${fmtDur(d.duration_target)}`;
    progBox = h("div", { class: "progress-wide" }, h("div", { class: "lbl" }, h("span", { text: `${label} · ${Math.floor(frac * 100)}%` }), h("span", { text: eta })), bar(frac));
  }
  const pg = d.progress || {};
  let cmpTiles = null;
  if (Object.keys(pg).some((k) => k.startsWith("cmp_"))) {
    const mib = (v) => v === undefined || v === null ? "–" : fmtInt(v);
    cmpTiles = h("div", {},
      h("p", { class: "tiles-title", text: `Table on the cluster — all nodes, from system_views (every ${(d.params || {}).monitor_every_s || "?"} s)` }),
      h("div", { class: "tiles" },
        tile("Disk used", mib(pg.cmp_disk_mib), "MiB"),
        tile("Largest partition", mib(pg.cmp_max_partition_mib), "MiB"),
        tile("Largest sstable", mib(pg.cmp_max_sstable_mib), "MiB"),
        tile("Tombstones / read", `${fmtInt(Math.round(pg.cmp_tombstones_p99 || 0))}`, `p99 · max ${fmtInt(Math.round(pg.cmp_tombstones_max || 0))}`),
        tile("Compacting now", fmtInt(pg.cmp_active || 0), pg.cmp_active_pct !== null && pg.cmp_active_pct !== undefined ? `${pg.cmp_active_pct}%` : ""),
        tile("Compactions done", fmtInt(pg.cmp_done || 0), pg.cmp_bytes_in ? `${fmtBytes(pg.cmp_bytes_in)} → ${fmtBytes(pg.cmp_bytes_out || 0)}` : "")));
  }

  // Stable skeleton per run: polling refreshes every section except the live controls,
  // so a slider being dragged is never detached from the DOM.
  if (box.dataset.run !== d.id) {
    box.dataset.run = d.id;
    box.replaceChildren(h("div", { id: "rd-top" }), h("div", { id: "rd-charts" }), h("div", { id: "rd-live" }), h("div", { id: "rd-bottom" }));
  }
  $("#rd-top").replaceChildren(head, progBox || "", tiles, cmpTiles || "");
  if (wl.kind !== "oneshot") {
    const c1 = h("canvas"), c2 = h("canvas");
    $("#rd-charts").replaceChildren(h("div", { class: "charts" },
      h("div", { class: "chart" }, h("h3", { class: "legend" }, "Throughput (ops/s)", h("span", {}, h("i", { class: "l1" }), "achieved"), h("span", {}, h("i", { class: "l2" }), "target")), c1),
      h("div", { class: "chart" }, h("h3", { class: "legend" }, "Latency (ms)", h("span", {}, h("i", { class: "l1" }), "p50"), h("span", {}, h("i", { class: "l3" }), "p99")), c2)));
    requestAnimationFrame(() => {
      drawChart(c1, state.series, [{ key: "rate", color: "--chart-1" }, { key: "target", color: "--chart-2", dash: true }]);
      drawChart(c2, state.series, [{ key: "p50", color: "--chart-1" }, { key: "p99", color: "--chart-3" }]);
    });
  }
  const live = $("#rd-live");
  if (active && wl.kind !== "oneshot") {
    if (!live.firstChild) {
      const liveParams = wl.params.filter((p) => p.live && !["rate", "concurrency"].includes(p.name));
      live.append(h("fieldset", { class: "live" }, h("legend", { text: "Live controls" }),
        rateWidget(d.rate_target, async (v) => { try { await api(`/api/runs/${d.id}`, { method: "PATCH", body: { rate: v } }); toast(`Rate → ${v === 0 ? "unlimited" : fmtRate(v) + " ops/s"}`); } catch (err) { toast(err.message); } }, { live: true, commitOnRelease: true }),
        h("div", { class: "params" },
          h("label", {}, "Concurrency", h("input", { type: "number", min: 1, max: 4096, value: d.concurrency, onchange: async (e) => { try { await api(`/api/runs/${d.id}`, { method: "PATCH", body: { concurrency: Number(e.target.value) } }); toast("Concurrency updated"); } catch (err) { toast(err.message); } } })),
          ...liveParams.map((p) => h("label", {}, `${p.label} (${p.min}–${p.max})`,
            h("input", { type: "range", min: p.min, max: p.max, value: (d.params || {})[p.name] ?? p.default, onchange: async (e) => { try { await api(`/api/runs/${d.id}`, { method: "PATCH", body: { params: { [p.name]: Number(e.target.value) } } }); toast(`${p.label} → ${e.target.value}`); } catch (err) { toast(err.message); } } }))))));
    }
  } else {
    live.replaceChildren();
  }
  const left = [];
  if (d.result) left.push(h("h3", { text: "Result" }), h("pre", { class: "json", text: JSON.stringify(d.result, null, 2) }));
  if (d.error) left.push(h("p", { class: "error", text: d.error }));
  const pdef = Object.fromEntries((wl.params || []).map((p) => [p.name, p]));
  const shown = Object.entries(d.params || {}).filter(([k]) => !pdef[k] || visible(pdef[k], d.params));  // hide other strategies' options
  left.push(h("h3", { text: "Parameters" }), h("dl", { class: "kv" }, shown.flatMap(([k, v]) => [h("dt", { text: k }), h("dd", { class: "mono", text: String(v) })])));
  const ev = h("div", { class: "events" }, (d.events || []).map((e) => h("div", { class: e.level }, `${new Date(e.t * 1000).toLocaleTimeString()} ${e.msg}`)));
  const old = box.querySelector(".events");
  const stick = !old || old.scrollTop + old.clientHeight >= old.scrollHeight - 4;
  const keep = old ? old.scrollTop : 0;
  $("#rd-bottom").replaceChildren(h("div", { class: "cols2" }, h("div", {}, left), h("div", {}, h("h3", { text: "Events" }), ev)));
  ev.scrollTop = stick ? ev.scrollHeight : keep;
}

/* ---------- tiny canvas line chart ---------- */
function drawChart(canvas, series, lines) {
  const css = getComputedStyle(document.documentElement);
  const dpr = window.devicePixelRatio || 1, w = canvas.clientWidth, hgt = canvas.clientHeight;
  canvas.width = w * dpr; canvas.height = hgt * dpr;
  const ctx = canvas.getContext("2d"); ctx.scale(dpr, dpr);
  const pts = series.slice(-300);
  const pad = { l: 46, r: 8, t: 8, b: 18 };
  const vals = [];
  for (const p of pts) for (const l of lines) { const v = p[l.key]; if (v) vals.push(v); }
  const max = Math.max(1, ...vals) * 1.1;
  ctx.font = "11px system-ui"; ctx.fillStyle = css.getPropertyValue("--muted"); ctx.strokeStyle = css.getPropertyValue("--border"); ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const y = pad.t + (hgt - pad.t - pad.b) * i / 4;
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(w - pad.r, y); ctx.stroke();
    ctx.fillText(fmtRate(max * (1 - i / 4)), 4, y + 4);
  }
  if (pts.length < 2) { ctx.fillText("waiting for data…", pad.l + 8, hgt / 2); return; }
  const t0 = pts[0].t, t1 = pts[pts.length - 1].t, span = Math.max(1, t1 - t0);
  ctx.fillText(`${fmtDur(span)} window`, w - pad.r - 70, hgt - 4);
  const X = (t) => pad.l + (w - pad.l - pad.r) * (t - t0) / span;
  const Y = (v) => pad.t + (hgt - pad.t - pad.b) * (1 - v / max);
  for (const l of lines) {
    ctx.strokeStyle = css.getPropertyValue(l.color); ctx.lineWidth = l.dash ? 1.2 : 2; ctx.setLineDash(l.dash ? [5, 4] : []);
    ctx.beginPath(); let started = false;
    for (const p of pts) {
      const v = p[l.key];
      if (v === undefined || v === null || (l.dash && !v)) continue;
      const x = X(p.t), y = Y(v);
      if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }
  ctx.setLineDash([]);
}

boot().catch((err) => { document.body.append(h("p", { class: "error", text: "Failed to load: " + err.message })); });
