/* 视图 7：批量处理与队列。 */
window.Views = window.Views || {};
window.Views.batch = (function () {
  const C = window.Common;
  let selected = new Set();
  let pollTimer = null;
  let expanded = new Set();   // 展开的任务 id（查看每张图的多路末端结果）

  return {
    mount(el) {
      el.innerHTML = `
        <div class="split">
          <div class="col">
            <div class="panel">
              <div class="panel-title">选择图像（可多选）</div>
              <div id="ba-gallery" style="max-height:340px;overflow:auto"></div>
              <div class="toolbar" style="margin-top:8px;margin-bottom:0">
                <span id="ba-count" class="badge">已选 0 张</span>
                <span class="spacer"></span>
                <button class="btn btn-sm" id="ba-clear">清空选择</button>
              </div>
            </div>
            <div class="panel">
              <div class="panel-title">选择流水线并入队</div>
              <div class="select-row">
                <select id="ba-pipeline"></select>
                <button class="btn btn-primary" id="ba-enqueue">入队处理</button>
              </div>
              <div class="hint" style="color:var(--text-faint);font-size:12px;margin-top:8px">后台线程池逐张处理，结果与历史自动记录，重复组合命中缓存。</div>
            </div>
          </div>
          <div class="col">
            <div class="panel"><div class="panel-title">任务队列</div><div id="ba-jobs"></div></div>
          </div>
        </div>`;

      loadImages(el);
      loadPipelines(el);
      loadJobs(el);

      el.querySelector("#ba-clear").onclick = () => { selected.clear(); el.querySelector("#ba-count").textContent = "已选 0 张"; markSelection(el); };
      el.querySelector("#ba-enqueue").onclick = async () => {
        const pid = el.querySelector("#ba-pipeline").value;
        if (!selected.size) { C.toast("请选择图像", "error"); return; }
        if (!pid) { C.toast("请选择流水线", "error"); return; }
        const r = await Api.post("/api/batch", { pipeline_id: pid, image_ids: Array.from(selected) });
        C.toast("已入队，任务 " + r.job_id.slice(0, 8), "success");
        loadJobs(el);
      };
    },

    refresh() { const el = document.querySelector('.view[data-view="batch"]'); if (el && this.mounted) { loadImages(el); loadPipelines(el); loadJobs(el); } },
  };

  async function loadImages(el) {
    const images = await C.fetchImages();
    const box = el.querySelector("#ba-gallery");
    box.innerHTML = C.galleryHTML(images);
    box.querySelectorAll(".card").forEach((card) => {
      card.addEventListener("click", () => {
        const id = card.dataset.id;
        if (selected.has(id)) selected.delete(id); else selected.add(id);
        card.classList.toggle("selected", selected.has(id));
        el.querySelector("#ba-count").textContent = `已选 ${selected.size} 张`;
      });
    });
    markSelection(el);
  }

  function markSelection(el) {
    const box = el.querySelector("#ba-gallery");
    box.querySelectorAll(".card").forEach((c) => c.classList.toggle("selected", selected.has(c.dataset.id)));
  }

  async function loadPipelines(el) {
    const ps = await C.fetchPipelines();
    el.querySelector("#ba-pipeline").innerHTML = `<option value="">— 选择流水线 —</option>` +
      ps.map((p) => `<option value="${p.id}">${C.esc(p.name)}</option>`).join("");
  }

  async function loadJobs(el) {
    const box = el.querySelector("#ba-jobs");
    let jobs = [];
    try { jobs = (await Api.get("/api/batch")).jobs; } catch (e) { box.innerHTML = `<div class="empty">加载失败</div>`; return; }
    if (!jobs.length) { box.innerHTML = `<div class="empty">暂无任务</div>`; return; }
    box.innerHTML = jobs.map((j) => {
      const pct = j.total ? Math.round(j.done / j.total * 100) : 0;
      const running = j.status === "queued" || j.status === "running";
      const badge = { done: "green", partial: "amber", cancelled: "amber", running: "", queued: "" }[j.status] || "red";
      const errCount = Object.values(j.results || {}).filter((r) => r.status === "error").length;
      // 多末端统计：每张图的每个 sink 各算一路结果
      const resultRows = Object.entries(j.results || {});
      const multiImages = resultRows.filter(([, r]) => (r.outputs || []).length > 1).length;
      const totalOutputs = resultRows.reduce((s, [, r]) => s + Math.max((r.outputs || []).length, r.result_id ? 1 : 0), 0);
      const isOpen = expanded.has(j.id);
      return `<div class="panel" style="margin-bottom:10px">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
          <span class="badge ${badge}">${statusText(j.status)}</span>
          <span style="font-weight:600">${C.esc(j.pipeline_name || "流水线")}</span>
          <span class="dim">${j.done}/${j.total}${errCount ? ` · ${errCount} 失败` : ""}</span>
          ${totalOutputs > resultRows.length ? `<span class="badge amber" title="每张图的每个末端节点都各保存了一路结果">${totalOutputs} 路结果 · ${multiImages} 张图含分叉</span>` : ""}
          <span style="flex:1"></span>
          ${resultRows.length ? `<button class="btn btn-sm" data-toggle="${j.id}">${isOpen ? "收起结果" : "查看结果"}</button>` : ""}
          ${running ? `<button class="btn btn-sm btn-danger" data-cancel="${j.id}">取消</button>` : ""}
        </div>
        <div class="progress"><div class="progress-bar" style="width:${pct}%"></div></div>
        <div class="keypoint-stats" style="margin-top:6px">${C.fmtDate(j.created_at)}</div>
        ${isOpen ? renderJobResults(resultRows) : ""}
      </div>`;
    }).join("");
    box.querySelectorAll("[data-cancel]").forEach((b) => b.onclick = async () => {
      await Api.post(`/api/batch/${b.dataset.cancel}/cancel`);
      C.toast("已请求取消", "success");
      loadJobs(el);
    });
    box.querySelectorAll("[data-toggle]").forEach((b) => b.onclick = () => {
      const id = b.dataset.toggle;
      if (expanded.has(id)) expanded.delete(id); else expanded.add(id);
      loadJobs(el);
    });

    const anyRunning = jobs.some((j) => j.status === "queued" || j.status === "running");
    if (anyRunning && !pollTimer) {
      pollTimer = setInterval(() => { if (!document.querySelector('.view[data-view="batch"]').classList.contains("active")) { clearInterval(pollTimer); pollTimer = null; } else loadJobs(el); }, 1500);
    } else if (!anyRunning && pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  }

  function statusText(s) {
    return { queued: "排队中", running: "处理中", done: "完成", partial: "部分失败", cancelled: "已取消", error: "错误" }[s] || s;
  }

  // 展开任务：逐张图列出其全部末端结果（分叉链一张图对应多路缩略图，按节点名标注）
  function renderJobResults(resultRows) {
    if (!resultRows.length) return "";
    const blocks = resultRows.map(([imageId, r]) => {
      // 新队列条目带 outputs；旧队列条目退化为单个 result_id
      let outs = (r.outputs || []).filter((o) => o && o.result_id);
      if (!outs.length && r.result_id) outs = [{ node_id: null, label: "结果", result_id: r.result_id }];
      const head = `
        <div class="batch-img-row">
          <span class="mono dim" title="${C.esc(imageId)}">${C.esc(imageId.slice(0, 10))}…</span>
          <span class="badge ${r.status === "ok" ? "green" : "red"}">${r.status === "ok" ? "成功" : "失败"}</span>
          ${outs.length > 1 ? `<span class="badge amber">${outs.length} 路末端</span>` : ""}
          ${r.error ? `<span class="dim" title="${C.esc(r.error)}">${C.esc(r.error.slice(0, 40))}</span>` : ""}
        </div>`;
      const body = outs.length ? `<div class="${outs.length > 1 ? "stage-grid batch-thumb-grid" : ""}">` +
        outs.map((o, i) => `
          <div class="output-card batch-thumb">
            <div class="output-title">
              ${i === 0 ? '<span class="badge green">主</span>' : ""}
              <span class="badge">${C.esc(o.label || ("节点 " + (o.node_id || "-")))}${o.node_id ? " #" + C.esc(o.node_id) : ""}</span>
              ${o.ok === false ? '<span class="badge red">失败</span>' : ""}
            </div>
            <a href="/api/results/${o.result_id}/file" target="_blank">
              <img src="/api/results/${o.result_id}/file" loading="lazy">
            </a>
          </div>`).join("") + `</div>` : '<div class="dim" style="font-size:12px">无结果图</div>';
      return `<div class="batch-img-block">${head}${body}</div>`;
    }).join("");
    return `<div class="batch-results">${blocks}</div>`;
  }
})();
