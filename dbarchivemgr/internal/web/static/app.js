/* DBArchiveManager 管理后台 —— 单页应用，数据全部来自 /api/v1，无构建、无外部依赖 */
"use strict";

/* ---------- 基础工具 ---------- */

const $ = (sel, el = document) => el.querySelector(sel);
const $$ = (sel, el = document) => Array.from(el.querySelectorAll(sel));
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (ch) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));

async function api(path, opts = {}) {
  const cfg = { headers: { "Content-Type": "application/json" }, ...opts };
  const res = await fetch("/api/v1" + path, cfg);
  if (res.status === 401) { location.href = "/auth/login"; throw new Error("未登录，跳转登录"); }
  let body;
  try { body = await res.json(); } catch { throw new Error(`HTTP ${res.status}（非 JSON 响应）`); }
  if (body.code !== 0) throw new Error(body.message || `HTTP ${res.status}`);
  return body.data;
}

function fmtBytes(n) {
  if (n == null) return "-";
  if (n < 1024) return n + " B";
  const units = ["KB", "MB", "GB", "TB"];
  let v = n, i = -1;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(1)} ${units[i]}`;
}
function fmtDate(v) { // RFC3339 → YYYY-MM-DD
  if (!v) return "-";
  const d = new Date(v);
  return isNaN(d) ? v : d.toISOString().slice(0, 10);
}
function fmtDT(v) { // RFC3339 → MM-DD HH:mm:ss
  if (!v) return "-";
  const d = new Date(v);
  if (isNaN(d)) return v;
  const p = (x) => String(x).padStart(2, "0");
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

const BADGES = {
  archiveStatus: {
    NOT_ARCHIVED: ["gray", "未归档"], ARCHIVING: ["blue", "归档中"],
    ARCHIVED: ["green", "已归档"], ARCHIVE_FAILED: ["red", "归档失败"],
  },
  uploadVerify: { PENDING: ["gray", "待校验"], VERIFIED: ["green", "已校验"], FAILED: ["red", "校验失败"] },
  transferStatus: { PENDING: ["gray", "待传输"], TRANSFERRING: ["blue", "传输中"], DONE: ["green", "完成"], FAILED: ["red", "失败"] },
  taskStatus: { PENDING: ["gray", "待执行"], RUNNING: ["blue", "执行中"], SUCCESS: ["green", "成功"], FAILED: ["red", "失败"] },
  inspectStatus: { RUNNING: ["blue", "进行中"], DONE: ["green", "完成"] },
};
function badge(kind, val) {
  const m = (BADGES[kind] && BADGES[kind][val]) || ["gray", val || "-"];
  return `<span class="badge ${m[0]}">${esc(m[1])}</span>`;
}
function editionName(e) { return e === "DISASTER_RECOVERY" ? "异地备" : e === "SAME_CITY" ? "同城备" : e; }

/* ---------- Toast / 确认弹窗 ---------- */

let toastTimer;
function toast(msg, type = "") {
  const el = $("#toast");
  el.textContent = msg;
  el.className = `toast ${type}`;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.add("hidden"), 4200);
}

function confirmBox(title, body, onOk) {
  $("#modal-title").textContent = title;
  $("#modal-body").textContent = body;
  const modal = $("#modal");
  modal.classList.remove("hidden");
  const ok = $("#modal-ok"), cancel = $("#modal-cancel");
  const close = () => { modal.classList.add("hidden"); ok.onclick = cancel.onclick = null; };
  cancel.onclick = close;
  ok.onclick = () => { close(); onOk(); };
}

// 通用表单编辑器：在 #modal 里渲染字段并调用 onSubmit(data)。onSubmit 抛错则显示在表单内。
function openEditor(title, fields, onSubmit) {
  const modal = $("#modal");
  const ok = $("#modal-ok"), cancel = $("#modal-cancel");
  $("#modal-title").textContent = title;
  $("#modal-body").innerHTML = `
    <div class="form">
      ${fields.map((f, i) => `
        <label class="field">
          <span class="field-label">${esc(f.label)}</span>
          <input data-field="${i}" type="${f.type || "text"}"
            value="${esc(f.value ?? "")}" ${f.ph ? `placeholder="${esc(f.ph)}"` : ""}>
        </label>`).join("")}
    </div>
    <div class="form-err hidden"></div>`;
  modal.classList.remove("hidden");
  ok.textContent = "保存";
  ok.classList.replace("danger", "primary");
  const restore = () => {
    modal.classList.add("hidden");
    ok.onclick = cancel.onclick = null;
    ok.textContent = "确认";
    ok.classList.replace("primary", "danger");
  };
  cancel.onclick = restore;
  ok.onclick = async () => {
    const data = {};
    fields.forEach((f, i) => { data[f.key] = $('[data-field="' + i + '"]', modal).value.trim(); });
    const errEl = $(".form-err", modal);
    try { await onSubmit(data); restore(); }
    catch (e) { errEl.textContent = e.message; errEl.classList.remove("hidden"); }
  };
}

/* ---------- Tab 路由与轮询 ---------- */

const POLL_MS = 5000;
let currentTab = "overview";
let pollTimer;
let expandedInspect = null; // 当前展开的巡检 ID（轮询重渲染后恢复）

const TABS = {
  overview:  { title: "概览", render: renderOverview },
  clusters:  { title: "集群拓扑", render: renderClusters },
  gfs:       { title: "机房管理", render: renderGfs },
  backups:   { title: "备份元数据", render: renderBackups },
  inspects:  { title: "巡检管理", render: renderInspects },
  replenish: { title: "补发管理", render: renderReplenish },
  archive:   { title: "归档管理", render: renderArchive },
  cleanup:   { title: "GFS 清理", render: renderCleanup },
  report:    { title: "备份上报", render: renderReport },
};

function switchTab(tab) {
  if (!TABS[tab]) return;
  currentTab = tab;
  $$(".nav-item").forEach((b) => b.classList.toggle("active", b.dataset.tab === tab));
  loadTab();
}

async function loadTab() {
  const content = $("#content");
  try {
    await TABS[currentTab].render(content);
  } catch (err) {
    content.innerHTML = `<div class="panel"><div class="empty-tip">加载失败：${esc(err.message)}</div></div>`;
  }
}

function startPolling() {
  clearInterval(pollTimer);
  pollTimer = setInterval(() => {
    // 含表单输入的 tab（补发/上报）不自动轮询，避免清空用户输入；其余 5s 刷新
    if (!["report", "replenish"].includes(currentTab)) loadTab();
  }, POLL_MS);
}

/* ---------- 事件委托 ---------- */

document.addEventListener("click", async (ev) => {
  const nav = ev.target.closest(".nav-item");
  if (nav) { switchTab(nav.dataset.tab); return; }

  const btn = ev.target.closest("[data-action]");
  if (!btn) return;
  const { action } = btn.dataset;
  try {
    btn.disabled = true;
    await ACTIONS[action]?.(btn);
  } catch (err) {
    toast(`操作失败：${err.message}`, "err");
  } finally {
    btn.disabled = false;
  }
});

const ACTIONS = {
  reload: () => { loadTab(); toast("已刷新", "ok"); },

  "inspect-run": async () => {
    const r = await api("/inspect/run", { method: "POST", body: "{}" });
    toast(`巡检完成：应检 ${r.totalSlots} 槽位，缺失 ${r.missingCount}，异常 ${r.brokenCount}`, "ok");
    await loadTab();
  },

  "inspect-detail": async (btn) => {
    const row = btn.closest("tr");
    const detail = row.nextElementSibling;
    if (detail && detail.classList.contains("detail-row")) { expandedInspect = null; detail.remove(); return; }
    await openInspectDetail(btn.dataset.id);
  },

  "replenish-one": async (btn) => {
    const body = {
      clusterId: +btn.dataset.cluster, dbId: +btn.dataset.db,
      dbName: btn.dataset.name, backupDate: btn.dataset.date, backupEdition: btn.dataset.edition,
    };
    const r = await api("/replenish/task", { method: "POST", body: JSON.stringify(body) });
    toast(`补发任务已下发：${r.taskId}（${r.status}）`, "ok");
    await loadTab();
  },

  "replenish-form": async () => {
    const read = (id) => $(`#${id}`).value.trim();
    const body = {
      clusterId: +read("rp-cluster"), dbId: +read("rp-db"),
      dbName: read("rp-name"), backupDate: read("rp-date"), backupEdition: read("rp-edition"),
    };
    if (!body.dbName || !body.backupDate) throw new Error("库名与备份日期必填");
    const r = await api("/replenish/task", { method: "POST", body: JSON.stringify(body) });
    toast(`补发任务已下发：${r.taskId}（${r.status}）`, "ok");
    await loadTab();
  },

  "rolling-generate": async () => {
    const r = await api("/archive/rolling-list/generate", { method: "POST", body: "{}" });
    toast(`滚动清单已生成并推送大数据：批次 ${r.archiveBatchId}，共 ${r.items.length} 条`, "ok");
    await loadTab();
  },

  "cleanup-run": () => new Promise((resolve) => {
    confirmBox("确认执行 GFS 清理？",
      "将删除「已归档且满该集群留存天数」的 GFS 备份文件（同城卷与异地卷）。该操作不可恢复。",
      async () => {
        try {
          const r = await api("/cleanup", { method: "POST", body: "{}" });
          toast(`清理完成：删除 ${r.deleted} 份备份文件`, "ok");
          await loadTab();
        } catch (err) { toast(`清理失败：${err.message}`, "err"); }
        resolve();
      });
  }),

  "report-form": async () => {
    const read = (id) => $(`#${id}`).value.trim();
    const body = {
      hostIp: read("rp2-ip"), dbName: read("rp2-name"), backupDate: read("rp2-date"),
      backupEdition: read("rp2-edition"), gfsPath: read("rp2-city"), peerGfsPath: read("rp2-dr"),
    };
    if (!body.hostIp || !body.dbName || !body.backupDate || !body.gfsPath) throw new Error("实例 IP、库名、备份日期、主卷路径必填");
    const r = await api("/backup/meta/report", { method: "POST", body: JSON.stringify(body) });
    toast(`上报成功：backupId=${r.backupId}，双卷实测 ${fmtBytes(r.fileSizeBytes)}，状态 ${r.archiveStatus}`, "ok");
  },

  "backup-filter": () => loadTab(),

  logout: () => { location.href = "/auth/logout"; },

  "cluster-add": async () => {
    await openEditor("新增集群（单条）", [
      { key: "clusterCode", label: "集群编码（必填，唯一）", value: "" },
      { key: "clusterName", label: "集群名称", value: "" },
      { key: "primaryRegion", label: "主地域", value: "" },
      { key: "drRegion", label: "异地备卷(DR)", value: "" },
      { key: "cityRegion", label: "同城备卷(City)", value: "" },
      { key: "archiveWindowDays", label: "归档窗口(天)", type: "number", value: "15" },
      { key: "retentionDays", label: "GFS留存(天，0=立即清，留空=默认10)", type: "number", value: "" },
      { key: "pathTemplate", label: "路径模板(留空用默认)", value: "", ph: "{root}/{cluster}/{db}/{date}/{vol}/full.sql.gz" },
    ], async (d) => {
      if (!d.clusterCode) throw new Error("集群编码必填");
      const r = await api("/cluster", { method: "POST", body: JSON.stringify({
        clusterCode: d.clusterCode, clusterName: d.clusterName,
        primaryRegion: d.primaryRegion, drRegion: d.drRegion, cityRegion: d.cityRegion,
        archiveWindowDays: d.archiveWindowDays === "" ? 15 : +d.archiveWindowDays,
        retentionDays: d.retentionDays === "" ? null : +d.retentionDays,
        pathTemplate: d.pathTemplate || null,
      }) });
      toast(`集群已新增：${r.clusterId}（${d.clusterCode}）`, "ok");
      await loadTab();
    });
  },

  "db-add": async (btn) => {
    const cid = btn.dataset.id;
    await openEditor("新增库到集群（单条）", [
      { key: "dbName", label: "库名（必填，集群内唯一）", value: "" },
      { key: "editions", label: "启用版本(comma分隔)", value: "DR,SAME_CITY" },
      { key: "archiveWindowDays", label: "归档窗口(天，留空默认15)", type: "number", value: "" },
      { key: "archiveTime", label: "错峰归档时间(如 08:00)", value: "" },
      { key: "pathTemplate", label: "路径模板(留空用集群/默认)", value: "", ph: "{root}/{cluster}/{db}/{date}/{vol}/full.sql.gz" },
    ], async (f) => {
      if (!f.dbName) throw new Error("库名必填");
      await api(`/cluster/${cid}/db`, { method: "POST", body: JSON.stringify({
        dbName: f.dbName, editions: f.editions || "DR,SAME_CITY",
        archiveWindowDays: f.archiveWindowDays === "" ? 15 : +f.archiveWindowDays,
        archiveTime: f.archiveTime || null, pathTemplate: f.pathTemplate || null,
      }) });
      toast("库已新增", "ok");
      await loadTab();
    });
  },

  "inst-add": async (btn) => {
    const cid = btn.dataset.id;
    await openEditor("新增实例 IP（单条）", [
      { key: "hostIp", label: "实例 IP（必填，全局唯一）", value: "" },
      { key: "hostName", label: "主机名", value: "" },
      { key: "role", label: "角色(如 MASTER/REPLICA)", value: "MASTER" },
    ], async (f) => {
      if (!f.hostIp) throw new Error("实例 IP 必填");
      const r = await api(`/cluster/${cid}/instance`, { method: "POST", body: JSON.stringify({
        hostIp: f.hostIp, hostName: f.hostName, role: f.role,
      }) });
      toast(`实例已新增：${r.hostIp}`, "ok");
      await loadTab();
    });
  },

  "gfs-endpoint-add": async () => {
    await openEditor("新增机房 GFS 管理端", [
      { key: "region", label: "机房名（须与集群 同城备卷/异地备卷 填写一致；* 为兜底）", value: "" },
      { key: "baseUrl", label: "GFS 管理端地址（必填）", value: "", ph: "http://gfs-hz:8200" },
      { key: "timeoutSeconds", label: "出站超时（秒，留空/0=默认30s）", value: "", type: "number", ph: "30" },
      { key: "remark", label: "备注", value: "" },
    ], async (f) => {
      if (!f.region || !f.baseUrl) throw new Error("机房名与管理端地址必填");
      await api("/gfs/endpoints", { method: "POST", body: JSON.stringify({
        region: f.region, baseUrl: f.baseUrl, timeoutSeconds: Number(f.timeoutSeconds) || 0, remark: f.remark || null,
      }) });
      toast(`机房端点已新增：${f.region} → ${f.baseUrl}（即时生效）`, "ok");
      await loadTab();
    });
  },

  "gfs-endpoint-edit": async (btn) => {
    const id = btn.dataset.id;
    const eps = await api("/gfs/endpoints");
    const e = (eps.endpoints || []).find((x) => String(x.endpointId) === String(id));
    if (!e) throw new Error("机房端点不存在");
    await openEditor(`编辑机房 ${e.region}`, [
      { key: "region", label: "机房名（* 为兜底）", value: e.region },
      { key: "baseUrl", label: "GFS 管理端地址", value: e.baseUrl },
      { key: "timeoutSeconds", label: "出站超时（秒，留空/0=默认30s）", value: e.timeoutSeconds || "", type: "number", ph: "30" },
      { key: "remark", label: "备注", value: e.remark || "" },
    ], async (f) => {
      if (!f.region || !f.baseUrl) throw new Error("机房名与管理端地址必填");
      await api(`/gfs/endpoints/${id}`, { method: "PUT", body: JSON.stringify({
        region: f.region, baseUrl: f.baseUrl, timeoutSeconds: Number(f.timeoutSeconds) || 0, remark: f.remark || null,
      }) });
      toast("机房端点已更新（即时生效）", "ok");
      await loadTab();
    });
  },

  "gfs-endpoint-del": (btn) => new Promise((resolve) => {
    confirmBox(`删除机房「${btn.dataset.name}」的管理端配置？`,
      "删除后该机房的 stat/delete 回落兜底端点（* 或 DBAM_GFS_MAP 兜底）；未配置任何兜底时按文件不存在处理。",
      async () => {
        try { await api(`/gfs/endpoints/${btn.dataset.id}`, { method: "DELETE" }); toast("机房端点已删除", "ok"); }
        catch (e) { toast(`删除失败：${e.message}`, "err"); }
        await loadTab(); resolve();
      });
  }),

  "cluster-edit": async (btn) => {
    const id = btn.dataset.id;
    const cl = await api(`/cluster/${id}`);
    await openEditor(`编辑集群 ${cl.clusterCode}`, [
      { key: "clusterName", label: "集群名称", value: cl.clusterName },
      { key: "primaryRegion", label: "主地域", value: cl.primaryRegion },
      { key: "drRegion", label: "异地备卷(DR)", value: cl.drRegion },
      { key: "cityRegion", label: "同城备卷(City)", value: cl.cityRegion },
      { key: "archiveWindowDays", label: "归档窗口(天)", type: "number", value: cl.archiveWindowDays },
      { key: "retentionDays", label: "GFS留存(天，0=归档后立即清)", type: "number", value: cl.retentionDays ?? "" },
      { key: "pathTemplate", label: "路径模板(留空用默认)", value: cl.pathTemplate ?? "", ph: "{root}/{cluster}/{db}/{date}/{vol}/full.sql.gz" },
    ], async (d) => {
      await api(`/cluster/${id}`, { method: "PUT", body: JSON.stringify({
        clusterName: d.clusterName, primaryRegion: d.primaryRegion,
        drRegion: d.drRegion, cityRegion: d.cityRegion,
        archiveWindowDays: d.archiveWindowDays === "" ? 15 : +d.archiveWindowDays,
        retentionDays: d.retentionDays === "" ? null : +d.retentionDays,
        pathTemplate: d.pathTemplate || null,
        editions: cl.editions, enabled: cl.enabled, // 保留 Save 覆盖不到的字段
      }) });
      toast("集群已更新", "ok");
      await loadTab();
    });
  },

  "cluster-delete": (btn) => new Promise((resolve) => {
    confirmBox(`删除集群「${btn.dataset.name}」？`,
      "将级联删除该集群下全部库与实例配置。备份元数据与归档历史保留，不会物理删除。",
      async () => {
        try { await api(`/cluster/${btn.dataset.id}`, { method: "DELETE" }); toast("集群已删除", "ok"); }
        catch (e) { toast(`删除失败：${e.message}`, "err"); }
        await loadTab(); resolve();
      });
  }),

  "db-edit": async (btn) => {
    const id = btn.dataset.id;
    const dbs = await api(`/dbs`);
    const d = dbs.find((x) => `${x.dbId}` === id);
    if (!d) throw new Error("未找到该库");
    await openEditor(`编辑库 ${d.dbName}`, [
      { key: "dbName", label: "库名", value: d.dbName },
      { key: "editions", label: "启用版本(comma分隔，如 DR,SAME_CITY)", value: d.editions },
      { key: "archiveWindowDays", label: "归档窗口(天，可覆盖集群)", type: "number", value: d.archiveWindowDays },
      { key: "archiveTime", label: "错峰归档时间(如 08:00)", value: d.archiveTime ?? "" },
      { key: "pathTemplate", label: "路径模板(留空用集群/默认)", value: d.pathTemplate, ph: "{root}/{cluster}/{db}/{date}/{vol}/full.sql.gz" },
    ], async (f) => {
      await api(`/db/${id}`, { method: "PUT", body: JSON.stringify({
        dbName: f.dbName, editions: f.editions,
        archiveWindowDays: f.archiveWindowDays === "" ? 0 : +f.archiveWindowDays,
        archiveTime: f.archiveTime || null, pathTemplate: f.pathTemplate || null,
        enabled: d.enabled, // 保留 Save 覆盖不到的字段
      }) });
      toast("库已更新", "ok");
      await loadTab();
    });
  },

  "db-delete": (btn) => new Promise((resolve) => {
    confirmBox(`删除库「${btn.dataset.name}」？`,
      "将删除该库的配置记录。其备份元数据与归档历史保留，不会物理删除。",
      async () => {
        try { await api(`/db/${btn.dataset.id}`, { method: "DELETE" }); toast("库已删除", "ok"); }
        catch (e) { toast(`删除失败：${e.message}`, "err"); }
        await loadTab(); resolve();
      });
  }),

  "inst-delete": (btn) => new Promise((resolve) => {
    confirmBox(`删除实例 IP「${btn.dataset.ip}」？`,
      "将移除该实例配置。此前已上报的备份不受影响。",
      async () => {
        try { await api(`/instance/${btn.dataset.id}`, { method: "DELETE" }); toast("实例已删除", "ok"); }
        catch (e) { toast(`删除失败：${e.message}`, "err"); }
        await loadTab(); resolve();
      });
  }),

  "csv-template": () => {
    const header = "type,clusterCode,clusterName,primaryRegion,drRegion,cityRegion,archiveWindowDays,retentionDays,pathTemplate,dbName,dbEditions,dbWindowDays,dbArchiveTime,dbPathTemplate,hostIp,hostName,role";
    // 每个数组与 header 列对齐：clusterCode(1) 关联；db/instance 行用它定位所属集群
    const row = (vals) => {
      const f = new Array(header.split(",").length).fill("");
      Object.entries(vals).forEach(([k, v]) => { f[+k] = v; });
      return f.join(",");
    };
    const rows = [
      header,
      row({ 0: "cluster", 1: "CL-AT-01", 2: "核心交易", 3: "杭州A", 4: "杭州B", 5: "杭州同城", 6: "15", 7: "10" }),
      row({ 0: "db", 1: "CL-AT-01", 9: "orders", 10: "DR;SAME_CITY", 11: "10", 12: "08:00" }),
      row({ 0: "instance", 1: "CL-AT-01", 14: "10.20.30.41", 15: "db-orders-01", 16: "MASTER" }),
    ];
    download("dbam_cluster_import_template.csv", "﻿" + rows.join("\n"));
    toast("已下载 CSV 模板", "ok");
  },
};

function download(name, text) {
  const url = URL.createObjectURL(new Blob([text], { type: "text/csv;charset=utf-8" }));
  const a = document.createElement("a");
  a.href = url; a.download = name;
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

// 静态 CSV 文件选择（input 放在 index.html，不随轮询重渲染，避免选中被清空）。
function initCsvImport() {
  const input = $("#csv-file");
  input.addEventListener("change", async () => {
    const file = input.files[0];
    input.value = ""; // 允许重复选择同一文件
    if (!file) return;
    if (file.size > 4 << 20) { toast("CSV 文件过大（上限 4MB）", "err"); return; }
    const fd = new FormData();
    fd.append("file", file);
    try {
      const res = await fetch("/api/v1/cluster/import", { method: "POST", body: fd });
      const body = await res.json();
      if (body.code !== 0) throw new Error(body.message);
      const d = body.data;
      const ok = !d.errors || !d.errors.length;
      let msg = `导入完成：集群 ${d.cluster} / 库 ${d.db} / 实例 ${d.instance}`;
      if (!ok) msg += `，失败 ${d.errors.length} 行（见控制台）`, console.warn(d.errors);
      toast(msg, ok ? "ok" : "err");
    } catch (err) { toast(`导入失败：${err.message}`, "err"); }
    await loadTab();
  });
}

/* ---------- 渲染：概览 ---------- */

async function renderOverview(content) {
  const [rec, sched, backups] = await Promise.all([
    api("/meta/reconciliation"), api("/schedule/status"), api("/backup/meta"),
  ]);
  const schedRow = (key, name) => {
    const j = sched[key];
    if (!j) return "";
    return `<tr>
      <td>${name}</td><td class="path">${esc(j.spec)}</td>
      <td class="num">${j.lastAt ? fmtDT(j.lastAt) : "未运行"}</td>
      <td class="num">${j.lastCount < 0 ? "-" : j.lastCount}</td>
      <td>${j.lastOK ? '<span class="badge green">成功</span>' : '<span class="badge red">失败</span>'}</td>
    </tr>`;
  };
  const byStatus = {};
  backups.forEach((b) => { byStatus[b.archiveStatus] = (byStatus[b.archiveStatus] || 0) + 1; });
  content.innerHTML = `
    <div class="card-row">
      <div class="stat-card total"><div class="num">${rec.total}</div><div class="label">备份元数据总数</div></div>
      <div class="stat-card archived"><div class="num">${rec.archived}</div><div class="label">已归档 ARCHIVED</div></div>
      <div class="stat-card notarchived"><div class="num">${rec.notArchived}</div><div class="label">未归档 NOT_ARCHIVED</div></div>
      <div class="stat-card failed"><div class="num">${rec.archiveFailed}</div><div class="label">归档失败 FAILED</div></div>
      <div class="stat-card"><div class="num">${byStatus.ARCHIVING || 0}</div><div class="label">归档中 ARCHIVING</div></div>
      <div class="stat-card"><div class="num">${backups.filter((b) => b.canDelete).length}</div><div class="label">可清理 canDelete</div></div>
    </div>
    <div class="panel">
      <div class="panel-head"><h3>定时调度</h3><span class="hint">每 ${POLL_MS / 1000}s 自动刷新</span></div>
      <table class="tbl">
        <tr><th>任务</th><th>cron 规格</th><th>最近运行</th><th>结果计数</th><th>状态</th></tr>
        ${schedRow("inspect", "完整性巡检 + 缺失补发")}
        ${schedRow("rolling", "滚动归档清单生成")}
      </table>
    </div>
    <div class="panel">
      <h3>常用操作</h3>
      <div style="display:flex;gap:10px;flex-wrap:wrap">
        <button class="btn" data-action="inspect-run">立即巡检</button>
        <button class="btn" data-action="rolling-generate">生成滚动归档清单</button>
        <button class="btn danger" data-action="cleanup-run">执行 GFS 清理</button>
      </div>
    </div>`;
}

/* ---------- 渲染：集群拓扑 ---------- */

async function renderClusters(content) {
  const clusters = await api("/clusters");
  const importArea = `
    <div class="panel" style="margin-bottom:16px">
      <div class="panel-head"><h3>集群管理</h3></div>
      <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
        <button class="btn" data-action="cluster-add">新增集群</button>
        <button class="btn" data-action="csv-template">下载 CSV 模板</button>
        <button class="btn" id="csv-import-btn">导入 CSV</button>
        <span class="hint">可单条新增集群/库/实例，或用 CSV「一行一实体」批量导入（clusterCode 关联，重复导入幂等）</span>
      </div>
    </div>`;
  if (!clusters.length) { content.innerHTML = importArea + emptyPanel("尚未注册任何集群,可用上方「新增集群」单条创建,或用「导入 CSV」批量导入"); return; }
  const cards = await Promise.all(clusters.map(async (cl) => {
    const [dbs, instances] = await Promise.all([
      api(`/dbs?clusterId=${cl.clusterId}`), api(`/cluster/${cl.clusterId}/instances`),
    ]);
    const dbRows = dbs.map((d) => `
      <tr><td>${esc(d.dbName)}</td><td>${esc(d.editions)}</td>
      <td class="num">${d.archiveWindowDays} 天</td><td class="num">${esc(d.archiveTime || "-")}</td>
      <td class="ops">
        <button class="link-btn" data-action="db-edit" data-id="${d.dbId}" data-name="${esc(d.dbName)}">编辑</button>
        <button class="link-btn danger" data-action="db-delete" data-id="${d.dbId}" data-name="${esc(d.dbName)}">删除</button>
      </td></tr>`).join("");
    const ipTags = instances.map((i) =>
      `<span class="ip-tag" title="${esc(i.hostName || "")} ${esc(i.role || "")}">${esc(i.hostIp)}
        <button class="ip-del" data-action="inst-delete" data-id="${i.instanceId}" data-ip="${esc(i.hostIp)}">×</button>
      </span>`).join(" ") || '<span class="hint">未挂载实例</span>';
    return `<div class="panel cluster-card">
      <div class="panel-head">
        <h3>${esc(cl.clusterName)}（${esc(cl.clusterCode)}）</h3>
        <div class="card-ops">
          ${cl.enabled ? '<span class="badge green">启用</span>' : '<span class="badge gray">停用</span>'}
          <button class="link-btn" data-action="cluster-edit" data-id="${cl.clusterId}">编辑</button>
          <button class="link-btn danger" data-action="cluster-delete" data-id="${cl.clusterId}" data-name="${esc(cl.clusterName)}">删除</button>
        </div>
        <div class="card-sub-ops">
          <button class="link-btn" data-action="db-add" data-id="${cl.clusterId}">+ 库</button>
          <button class="link-btn" data-action="inst-add" data-id="${cl.clusterId}">+ 实例</button>
        </div>
      </div>
      <div class="kv" style="margin-bottom:12px">
        <span class="k">主地域</span><span class="v">${esc(cl.primaryRegion)}</span>
        <span class="k">异地备卷(DR)</span><span class="v">${esc(cl.drRegion)}</span>
        <span class="k">同城备卷(City)</span><span class="v">${esc(cl.cityRegion)}</span>
        <span class="k">归档窗口</span><span class="v">${cl.archiveWindowDays} 天（库级可覆盖）</span>
        <span class="k">GFS 留存</span><span class="v">${cl.retentionDays ?? "默认 10"} 天（归档后满留存可清理）</span>
        <span class="k">路径模板</span><span class="v">${esc(cl.pathTemplate || "全局默认 {root}/{cluster}/{db}/{date}/{vol}/full.sql.gz")}</span>
        <span class="k">实例 IP</span><span class="v">${ipTags}</span>
      </div>
      <table class="tbl"><tr><th>库</th><th>启用版本</th><th>归档窗口</th><th>错峰归档时间</th><th></th></tr>${dbRows}</table>
    </div>`;
  }));
  content.innerHTML = importArea + cards.join("");
  const btn = $("#csv-import-btn");
  if (btn) btn.onclick = () => $("#csv-file").click();
}

/* ---------- 渲染：机房管理 ---------- */

async function renderGfs(content) {
  const [eps, clusters] = await Promise.all([api("/gfs/endpoints"), api("/clusters")]);
  const list = eps.endpoints || [];
  // 统计每个机房端点被多少集群的 cityRegion/drRegion 引用
  const usedBy = {};
  (clusters || []).forEach((c) => {
    [c.cityRegion, c.drRegion].filter(Boolean).forEach((r) => {
      usedBy[r] = (usedBy[r] || 0) + 1;
    });
  });
  const rows = list.map((e) => `
    <tr>
      <td class="num">${e.endpointId}</td>
      <td>${esc(e.region)}</td>
      <td class="path">${esc(e.baseUrl)}</td>
      <td class="num">${e.timeoutSeconds || 30}s</td>
      <td>${esc(e.remark || "-")}</td>
      <td>${usedBy[e.region] ? `${usedBy[e.region]} 个集群` : '<span class="hint">未被集群引用</span>'}</td>
      <td class="ops">
        <button class="link-btn" data-action="gfs-endpoint-edit" data-id="${e.endpointId}">编辑</button>
        <button class="link-btn danger" data-action="gfs-endpoint-del" data-id="${e.endpointId}" data-name="${esc(e.region)}">删除</button>
      </td>
    </tr>`).join("");
  content.innerHTML = `
    <div class="panel">
      <div class="panel-head">
        <h3>机房 GFS 管理端</h3>
        <div class="card-ops">
          <span class="hint">库表配置优先于启动参数 -gfs-map；机房名须与集群的 同城备卷(City)/异地备卷(DR) 填写一致</span>
          <button class="btn" data-action="gfs-endpoint-add">新增机房端点</button>
        </div>
      </div>
      ${list.length ? `
      <table class="tbl">
        <thead><tr><th>ID</th><th>机房</th><th>GFS 管理端地址</th><th>超时</th><th>备注</th><th>引用</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>` : `<div class="empty-tip">尚未配置机房端点：当前未配置 GFS 管理端。新增「机房→管理端」后（机房名填 * 即单地址模式），stat/delete 与归档/补发通知即按机房路由。
        在此新增「机房→GFS 管理端」后，管控将按集群的 cityRegion/drRegion 分发双卷的 stat/delete。</div>`}
    </div>`;
}

/* ---------- 渲染：备份元数据 ---------- */

async function renderBackups(content) {
  const qs = new URLSearchParams(location.hash.slice(1));
  const fCluster = qs.get("clusterId") || "";
  const fStatus = qs.get("archiveStatus") || "";
  const [clusters, backups] = await Promise.all([
    api("/clusters"),
    api(`/backup/meta?${qs.toString()}`),
  ]);
  const optCl = clusters.map((c) =>
    `<option value="${c.clusterId}" ${String(c.clusterId) === fCluster ? "selected" : ""}>${esc(c.clusterName)}</option>`).join("");
  const statuses = ["NOT_ARCHIVED", "ARCHIVING", "ARCHIVED", "ARCHIVE_FAILED"];
  const optSt = statuses.map((s) =>
    `<option value="${s}" ${s === fStatus ? "selected" : ""}>${s}</option>`).join("");
  const rows = backups.map((b) => `
    <tr>
      <td class="num">${b.backupId}</td>
      <td>${esc(b.dbName)}</td>
      <td class="num">${fmtDate(b.backupDate)}</td>
      <td>${esc(editionName(b.backupEdition))}</td>
      <td class="path" title="${esc(b.gfsPath)}\n${esc(b.peerGfsPath)}">
        主卷 ${fmtBytes(b.fileSizeBytes)} / 备卷 ${fmtBytes(b.peerFileSizeBytes)}</td>
      <td>${badge("uploadVerify", b.uploadVerify)}</td>
      <td>${badge("archiveStatus", b.archiveStatus)}</td>
      <td class="num">${b.archivedAt ? fmtDT(b.archivedAt) : "-"}</td>
      <td>${b.canDelete ? '<span class="badge orange">可清理</span>' : '<span class="badge gray">留存中</span>'}</td>
      <td>${b.deletedAt ? '<span class="badge red">已删除</span>' : "-"}</td>
    </tr>`).join("");
  content.innerHTML = `
    <div class="panel">
      <div class="panel-head"><h3>备份元数据（${backups.length} 条）</h3><span class="hint">唯一维度：集群+库+日期+版本</span></div>
      <div class="filter-bar">
        <label>集群</label><select id="f-cluster"><option value="">全部</option>${optCl}</select>
        <label>归档状态</label><select id="f-status"><option value="">全部</option>${optSt}</select>
        <button class="btn sm" data-action="backup-filter">筛选</button>
      </div>
      <table class="tbl">
        <tr><th>ID</th><th>库</th><th>备份日期</th><th>版本</th><th>双卷大小</th><th>上传校验</th><th>归档状态</th><th>归档时间</th><th>清理判定</th><th>删除</th></tr>
        ${rows || '<tr><td colspan="10" class="empty-tip">无数据</td></tr>'}
      </table>
    </div>`;
  $("#f-cluster").onchange = (e) => setBackupsFilter({ clusterId: e.target.value });
  $("#f-status").onchange = (e) => setBackupsFilter({ archiveStatus: e.target.value });
}
function setBackupsFilter(patch) {
  const qs = new URLSearchParams(location.hash.slice(1));
  Object.entries(patch).forEach(([k, v]) => (v ? qs.set(k, v) : qs.delete(k)));
  location.hash = qs.toString();
  loadTab();
}

/* ---------- 渲染：巡检管理 ---------- */

async function renderInspects(content) {
  const inspects = await api("/inspects?limit=20");
  const rows = inspects.map((i) => `
    <tr>
      <td class="num">${i.inspectId}</td>
      <td class="num">${fmtDT(i.inspectDate)}</td>
      <td class="num">${fmtDate(i.targetDate)}</td>
      <td class="num">${i.totalSlots}</td>
      <td>${i.okCount ? `<span class="badge green">${i.okCount}</span>` : "-"}</td>
      <td>${i.missingCount ? `<span class="badge red">${i.missingCount}</span>` : "0"}</td>
      <td>${i.brokenCount ? `<span class="badge orange">${i.brokenCount}</span>` : "0"}</td>
      <td>${badge("inspectStatus", i.status)}</td>
      <td><button class="btn sm ghost ${expandedInspect === i.inspectId ? "active" : ""}" data-action="inspect-detail" data-id="${i.inspectId}">缺失明细</button></td>
    </tr>`).join("");
  content.innerHTML = `
    <div class="panel">
      <div class="panel-head">
        <h3>巡检历史（最近 ${inspects.length} 次）</h3>
        <button class="btn" data-action="inspect-run">立即巡检</button>
      </div>
      <table class="tbl">
        <tr><th>ID</th><th>巡检时间</th><th>目标日期</th><th>应检槽位</th><th>正常</th><th>缺失</th><th>空文件</th><th>状态</th><th>操作</th></tr>
        ${rows || '<tr><td colspan="9" class="empty-tip">尚无巡检记录</td></tr>'}
      </table>
    </div>`;
  if (expandedInspect != null) {
    const btn = $(`[data-action="inspect-detail"][data-id="${expandedInspect}"]`);
    if (btn) await openInspectDetail(btn.dataset.id);
  }
}

async function openInspectDetail(id) {
  $$(".detail-row").forEach((d) => d.remove()); // 只展开一行
  expandedInspect = id;
  const r = await api(`/inspect/${id}`);
  const missing = (r.missing || []).map((m) => `
    <tr>
      <td>${esc(m.dbName)}</td>
      <td class="num">${fmtDate(m.backupDate)}</td>
      <td>${esc(editionName(m.backupEdition))}</td>
      <td>${m.missingType === "BROKEN" ? '<span class="badge orange">空文件</span>' : '<span class="badge red">缺失</span>'}</td>
      <td><button class="btn sm" data-action="replenish-one"
        data-cluster="${m.clusterId}" data-db="${m.dbId}" data-name="${esc(m.dbName)}"
        data-date="${fmtDate(m.backupDate)}" data-edition="${esc(m.backupEdition)}">下发补发</button></td>
    </tr>`).join("");
  const row = $(`[data-action="inspect-detail"][data-id="${id}"]`).closest("tr");
  row.insertAdjacentHTML("afterend", `
    <tr class="detail-row"><td colspan="8">
      <div class="missing-box">
        ${missing ? `<table class="tbl"><tr><th>库</th><th>备份日期</th><th>版本</th><th>缺失类型</th><th>操作</th></tr>${missing}</table>`
                  : '<div class="empty">本次巡检无缺失</div>'}
      </div>
    </td></tr>`);
}

/* ---------- 渲染：补发管理 ---------- */

async function renderReplenish(content) {
  const tasks = await api("/replenish/tasks");
  const rows = tasks.map((t) => `
    <tr>
      <td class="path">${esc(t.taskId)}</td>
      <td>${esc(t.dbName)}</td>
      <td class="num">${fmtDate(t.backupDate)}</td>
      <td>${esc(editionName(t.backupEdition))}</td>
      <td>${badge("taskStatus", t.status)}</td>
      <td class="num">${t.retryCount}/${t.maxRetry}</td>
      <td class="num">${fmtBytes(t.fileSizeBytes)}</td>
      <td class="num">${fmtDT(t.createdAt)}</td>
    </tr>`).join("");
  content.innerHTML = `
    <div class="panel">
      <div class="panel-head"><h3>手动下发补发</h3>
        <span class="hint">巡检缺失项可在「巡检管理」缺失明细中一键补发</span></div>
      <div class="form-grid">
        <div class="form-item"><label>集群 ID</label><input id="rp-cluster" type="number" min="1" placeholder="如 1" /></div>
        <div class="form-item"><label>库 ID</label><input id="rp-db" type="number" min="1" placeholder="如 1" /></div>
        <div class="form-item"><label>库名</label><input id="rp-name" placeholder="如 orders" /></div>
        <div class="form-item"><label>备份日期</label><input id="rp-date" type="date" /></div>
        <div class="form-item"><label>备份版本</label>
          <select id="rp-edition">
            <option value="DISASTER_RECOVERY">异地备 DISASTER_RECOVERY</option>
            <option value="SAME_CITY">同城备 SAME_CITY</option>
          </select></div>
      </div>
      <div class="form-actions"><button class="btn" data-action="replenish-form">下发补发指令</button></div>
    </div>
    <div class="panel">
      <div class="panel-head"><h3>补发任务（${tasks.length}）</h3><span class="hint">同一维度未完成不重复下发（防抖）</span></div>
      <table class="tbl">
        <tr><th>任务 ID</th><th>库</th><th>备份日期</th><th>版本</th><th>状态</th><th>重试</th><th>文件大小</th><th>创建时间</th></tr>
        ${rows || '<tr><td colspan="8" class="empty-tip">暂无补发任务</td></tr>'}
      </table>
    </div>`;
}

/* ---------- 渲染：归档管理 ---------- */

async function renderArchive(content) {
  const [batches, items] = await Promise.all([
    api("/archive/batches"), api("/backup/meta?archiveStatus=ARCHIVED"),
  ]);
  const batchRows = batches.map((b) => `
    <tr>
      <td class="path">${esc(b.archiveBatchId)}</td>
      <td class="num">${fmtDT(b.generateTime)}</td>
      <td class="num">${fmtDate(b.cutoffDate)}</td>
      <td class="num">${b.windowDays} 天</td>
      <td class="num">${b.itemCount}</td>
      <td>${b.status === "DONE" ? '<span class="badge green">完成</span>' : `<span class="badge blue">${esc(b.status)}</span>`}</td>
    </tr>`).join("");
  const archivedRows = items.slice(0, 50).map((b) => `
    <tr>
      <td class="num">${b.backupId}</td>
      <td>${esc(b.dbName)}</td>
      <td class="num">${fmtDate(b.backupDate)}</td>
      <td class="path" title="${esc(b.hdfsPath)}">${esc(b.hdfsPath || "-")}</td>
      <td class="num">${fmtBytes(b.hdfsSizeBytes)}</td>
      <td class="num">${b.archivedAt ? fmtDT(b.archivedAt) : "-"}</td>
      <td class="path">${esc(b.archiveBatchId || "-")}</td>
    </tr>`).join("");
  content.innerHTML = `
    <div class="panel">
      <div class="panel-head">
        <h3>归档批次</h3>
        <button class="btn" data-action="rolling-generate">生成滚动归档清单</button>
      </div>
      <table class="tbl">
        <tr><th>批次 ID</th><th>生成时间</th><th>截止日期</th><th>窗口</th><th>条目数</th><th>状态</th></tr>
        ${batchRows || '<tr><td colspan="6" class="empty-tip">暂无归档批次</td></tr>'}
      </table>
      <p class="hint" style="margin-top:8px">清单生成后经 HTTP 推送大数据组件，由其自治拉取 GFS → 写 HDFS → 回调管控；元数据去重，每份备份只归档一次。</p>
    </div>
    <div class="panel">
      <div class="panel-head"><h3>已归档备份（${items.length} 条，最多展示 50）</h3></div>
      <table class="tbl">
        <tr><th>ID</th><th>库</th><th>备份日期</th><th>HDFS 路径</th><th>HDFS 大小</th><th>归档时间</th><th>批次</th></tr>
        ${archivedRows || '<tr><td colspan="7" class="empty-tip">暂无已归档备份</td></tr>'}
      </table>
    </div>`;
}

/* ---------- 渲染：GFS 清理 ---------- */

async function renderCleanup(content) {
  const backups = await api("/backup/meta");
  const canDel = backups.filter((b) => b.canDelete);
  const rows = canDel.map((b) => `
    <tr>
      <td class="num">${b.backupId}</td>
      <td>${esc(b.dbName)}</td>
      <td class="num">${fmtDate(b.backupDate)}</td>
      <td>${badge("archiveStatus", b.archiveStatus)}</td>
      <td class="num">${b.archivedAt ? fmtDT(b.archivedAt) : "-"}</td>
      <td>${b.deletedAt ? '<span class="badge red">已删除</span>' : '<span class="badge orange">待清理</span>'}</td>
    </tr>`).join("");
  content.innerHTML = `
    <div class="panel">
      <div class="panel-head">
        <h3>GFS 清理</h3>
        <button class="btn danger" data-action="cleanup-run">执行清理</button>
      </div>
      <p class="hint" style="margin-bottom:12px">
        清理规则：仅删除「已归档 <b>且</b> 距归档已满该集群留存天数」的文件（同城卷与异地卷同删）；未归档、未满留存的一律保留。
      </p>
      <table class="tbl">
        <tr><th>ID</th><th>库</th><th>备份日期</th><th>归档状态</th><th>归档时间</th><th>清理状态</th></tr>
        ${rows || '<tr><td colspan="6" class="empty-tip">当前无可清理项（均已删除或未满留存）</td></tr>'}
      </table>
    </div>`;
}

/* ---------- 渲染：备份上报（演示辅助） ---------- */

async function renderReport(content) {
  const today = new Date().toISOString().slice(0, 10);
  content.innerHTML = `
    <div class="panel">
      <div class="panel-head"><h3>上报备份元数据</h3>
        <span class="hint">模拟数据库侧上报；真实环境由 DB 侧程序自动调用</span></div>
      <div class="form-grid">
        <div class="form-item"><label>来源实例 IP（须已挂载到某集群）</label><input id="rp2-ip" placeholder="如 10.0.1.11" /></div>
        <div class="form-item"><label>库名</label><input id="rp2-name" placeholder="如 orders" /></div>
        <div class="form-item"><label>备份日期</label><input id="rp2-date" type="date" value="${today}" /></div>
        <div class="form-item"><label>备份版本</label>
          <select id="rp2-edition">
            <option value="SAME_CITY">同城备 SAME_CITY</option>
            <option value="DISASTER_RECOVERY">异地备 DISASTER_RECOVERY</option>
          </select></div>
        <div class="form-item"><label>主卷路径（gfsPath）</label><input id="rp2-city" placeholder="/gfs/<ip>/<db>/<date>/city/full.sql.gz" /></div>
        <div class="form-item"><label>另一卷路径（peerGfsPath）</label><input id="rp2-dr" placeholder="/gfs/<ip>/<db>/<date>/dr/full.sql.gz" /></div>
      </div>
      <div class="form-actions"><button class="btn" data-action="report-form">上报</button></div>
      <p class="hint" style="margin-top:10px">
        上传判定：双卷文件「存在 + size &gt; 10MB」才判 VERIFIED；演示桩中需先在 GFS 写入文件，或复用种子数据的已有路径。
      </p>
    </div>`;
}

function emptyPanel(text) {
  return `<div class="panel"><div class="empty-tip">${esc(text)}</div></div>`;
}

/* ---------- 启动 ---------- */

function tickClock() {
  const p = (x) => String(x).padStart(2, "0");
  const d = new Date();
  $("#clock").textContent = `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}
setInterval(tickClock, 1000);
tickClock();
initUser();
initCsvImport();
startPolling();
loadTab();

// 顶栏登录态：拉 /auth/user 展示当前用户；未登录(401)整页跳 CAS 登录
async function initUser() {
  try {
    const res = await fetch("/auth/user");
    if (res.status === 401) { location.href = "/auth/login"; return; }
    const body = await res.json();
    const u = body.data && body.data.user;
    if (u) {
      $("#user-chip").textContent = "👤 " + u;
      $("#user-chip").classList.remove("hidden");
      $("#logout-btn").classList.remove("hidden");
    }
  } catch { /* /auth/user 不可达时不阻塞页面 */ }
}
