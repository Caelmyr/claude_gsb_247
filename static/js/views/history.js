/* 视图 10：历史记录与版本。 */
window.Views = window.Views || {};
window.Views.history = (function () {
  const C = window.Common;
  let current = null;

  return {
    mount(el) {
      el.innerHTML = `
        <div class="split">
          <div class="col" style="flex:1.6">
            <div class="panel">
              <div class="panel-title">处理历史<span class="dim">每条含当时的流水线快照（可恢复版本）</span></div>
              <div id="hi-list"></div>
            </div>
          </div>
          <div class="col">
            <div class="panel"><div class="panel-title">记录详情</div><div id="hi-detail"><div class="empty">选择左侧记录查看</div></div></div>
          </div>
        </div>`;
      load(el);
    },
    refresh() { const el = document.querySelector('.view[data-view="history"]'); if (el && this.mounted) load(el); },
  };

  async function load(el) {
    const r = await Api.get("/api/history");
    const list = r.history || [];
    const box = el.querySelector("#hi-list");
    if (!list.length) { box.innerHTML = `<div class="empty"><span class="big">🕘</span>暂无历史记录</div>`; return; }
    box.innerHTML = `<table class="table"><thead><tr>
      <th>时间</th><th>图像</th><th>流水线</th><th>节点</th><th>结果</th><th>耗时</th><th>状态</th><th></th>
    </tr></thead><tbody>` + list.map((e) => {
      const outN = (e.outputs || []).length;
      return `
      <tr data-id="${e.id}" style="cursor:pointer">
        <td class="mono">${C.fmtDate(e.created_at)}</td>
        <td title="${C.esc(e.image_name)}">${C.esc((e.image_name || "").slice(0, 18))}</td>
        <td>${C.esc(e.pipeline_name || "临时")}</td>
        <td>${e.node_count}</td>
        <td>${outN > 1 ? `<span class="badge amber" title="该链有多个末端，每路结果都已保存">${outN} 路</span>` : `<span class="dim">1</span>`}</td>
        <td>${C.fmtMs(e.duration_ms)}</td>
        <td><span class="badge ${e.status === "ok" ? "green" : "red"}">${e.status === "ok" ? "成功" : "失败"}</span>${e.cache_hit ? ' <span class="badge">缓存</span>' : ""}</td>
        <td><button class="btn btn-sm" data-id="${e.id}">查看</button></td>
      </tr>`;
    }).join("") + `</tbody></table>`;

    box.querySelectorAll("tr[data-id]").forEach((tr) => {
      tr.onclick = () => {
        current = list.find((x) => x.id === tr.dataset.id);
        box.querySelectorAll("tr").forEach((x) => x.style.background = "");
        tr.style.background = "var(--bg-hover)";
        renderDetail(el, current);
      };
    });
    if (current) renderDetail(el, current);
  }

  function renderDetail(el, e) {
    const box = el.querySelector("#hi-detail");
    const nodes = (e.pipeline_snapshot && e.pipeline_snapshot.nodes) || [];
    const nodeResults = e.node_results || [];
    // outputs：新记录是多末端数组；旧记录只有 result_id，做兼容回退
    let outputs = (e.outputs || []).filter((o) => o && o.result_id);
    if (!outputs.length && e.result_id) {
      outputs = [{ node_id: null, label: "结果", result_id: e.result_id }];
    }
    const outN = outputs.length;
    box.innerHTML = `
      <div class="keypoint-stats" style="line-height:1.9">
        <div><span class="dim">状态</span> <span class="badge ${e.status === "ok" ? "green" : "red"}">${e.status === "ok" ? "成功" : "失败"}</span></div>
        <div><span class="dim">图像</span> ${C.esc(e.image_name || "-")}</div>
        <div><span class="dim">流水线</span> ${C.esc(e.pipeline_name || "临时")}（${e.node_count} 节点）</div>
        <div><span class="dim">末端结果</span> <span class="badge ${outN > 1 ? "amber" : "green"}">${outN} 路</span>${outN > 1 ? ' <span class="dim">每个末端节点各一路，均已保存</span>' : ""}</div>
        <div><span class="dim">耗时</span> ${C.fmtMs(e.duration_ms)} · <span class="dim">缓存</span> ${e.cache_hit ? "命中" : "计算"}</div>
        ${e.error ? `<div><span class="dim">错误</span> ${C.esc(e.error)}</div>` : ""}
        <div><span class="dim">版本快照</span> ${nodes.map((n) => `<span class="badge">${C.esc(n.type)}</span>`).join(" ") || "无节点"}</div>
        ${nodeResults.length ? `<div><span class="dim">节点执行</span> ${nodeResults.map((n) => `${n.ok ? "✓" : "✗"}${n.node_id}`).join(" ")}</div>` : ""}
      </div>
      ${outputs.length ? `
        <div class="multi-output-banner" style="margin-top:10px">本记录共保存 ${outN} 路末端结果${outN > 1 ? "（按末端节点区分）" : ""}：</div>
        <div class="${outN > 1 ? "stage-grid" : ""}" style="margin-top:8px">
          ${outputs.map((o, i) => `
            <div class="output-card">
              <div class="output-title">
                ${i === 0 ? '<span class="badge green">主结果</span>' : ""}
                <span class="badge">${C.esc(o.label || ("节点 " + (o.node_id || "-")))}${o.node_id ? " #" + C.esc(o.node_id) : ""}</span>
                ${o.ok === false ? '<span class="badge red">节点失败</span>' : ""}
                ${o.cache_hit ? '<span class="badge">缓存</span>' : ""}
              </div>
              <img src="/api/results/${o.result_id}/file" style="width:100%;border-radius:8px">
            </div>`).join("")}
        </div>` : ""}
      <div class="toolbar" style="margin-top:12px">
        <button class="btn" id="hi-restore">恢复为流水线</button>
        <button class="btn btn-danger" id="hi-del">删除记录</button>
      </div>`;
    box.querySelector("#hi-restore").onclick = async () => {
      const p = await Api.post(`/api/history/${e.id}/restore`);
      C.toast("已恢复为流水线：" + p.name, "success");
      await C.refreshPipelines();
    };
    box.querySelector("#hi-del").onclick = async () => {
      if (!confirm("删除该历史记录（连同结果文件）？")) return;
      await Api.del(`/api/history/${e.id}`);
      current = null;
      C.toast("已删除", "success");
      load(document.querySelector('.view[data-view="history"]'));
    };
  }
})();
