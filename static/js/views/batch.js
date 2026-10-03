/* 视图 7：批量处理与队列。 */
window.Views = window.Views || {};
window.Views.batch = (function () {
  const C = window.Common;
  let selected = new Set();
  let pollTimer = null;

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
      // 多末端流水线：统计本次任务实际产出的结果分支总数，防止「白跑不知道」
      const branchTotal = Object.values(j.results || {}).reduce((s, r) => s + (r.output_count || (r.result_id ? 1 : 0)), 0);
      return `<div class="panel" style="margin-bottom:10px">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
          <span class="badge ${badge}">${statusText(j.status)}</span>
          <span style="font-weight:600">${C.esc(j.pipeline_name || "流水线")}</span>
          <span class="dim">${j.done}/${j.total}${errCount ? ` · ${errCount} 失败` : ""}</span>
          ${branchTotal ? `<span class="badge" title="本次任务每张图的每个末端各产生一个结果">共 ${branchTotal} 个末端结果</span>` : ""}
          <span style="flex:1"></span>
          ${j.done ? `<button class="btn btn-sm" data-toggle="${j.id}">明细</button>` : ""}
          ${running ? `<button class="btn btn-sm btn-danger" data-cancel="${j.id}">取消</button>` : ""}
        </div>
        <div class="progress"><div class="progress-bar" style="width:${pct}%"></div></div>
        <div class="keypoint-stats" style="margin-top:6px">${C.fmtDate(j.created_at)}</div>
        <div class="job-detail" data-detail="${j.id}" style="display:none;margin-top:10px"></div>
      </div>`;
    }).join("");
    box.querySelectorAll("[data-cancel]").forEach((b) => b.onclick = async () => {
      await Api.post(`/api/batch/${b.dataset.cancel}/cancel`);
      C.toast("已请求取消", "success");
      loadJobs(el);
    });
    box.querySelectorAll("[data-toggle]").forEach((b) => {
      b.onclick = () => {
        const detail = box.querySelector(`[data-detail="${b.dataset.toggle}"]`);
        if (!detail) return;
        if (detail.style.display === "none") {
          detail.style.display = "";
          b.textContent = "收起";
          renderJobDetail(detail, jobs.find((x) => x.id === b.dataset.toggle));
        } else {
          detail.style.display = "none";
          b.textContent = "明细";
        }
      };
    });

    const anyRunning = jobs.some((j) => j.status === "queued" || j.status === "running");
    if (anyRunning && !pollTimer) {
      pollTimer = setInterval(() => { if (!document.querySelector('.view[data-view="batch"]').classList.contains("active")) { clearInterval(pollTimer); pollTimer = null; } else loadJobs(el); }, 1500);
    } else if (!anyRunning && pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  }

  function statusText(s) {
    return { queued: "排队中", running: "处理中", done: "完成", partial: "部分失败", cancelled: "已取消", error: "错误" }[s] || s;
  }

  // 逐张图展开：同一张图的多个末端结果横向并列，标注末端节点与支路径
  function renderJobDetail(box, job) {
    const rows = Object.entries(job.results || {}).map(([imageId, r]) => {
      const outs = r.outputs && r.outputs.length
        ? r.outputs
        : (r.result_id ? [{ index: 0, label: "结果", path: [], ok: true, result_id: r.result_id }] : []);
      const cells = outs.length ? outs.map((o) => {
        const crumbs = (o.path || []).map((p) => C.esc(p.type)).join(" → ");
        return `<div class="branch-cell ${o.ok ? "" : "bad"}" title="${crumbs}">
          ${o.ok
            ? `<img src="/api/results/${o.result_id}/file" loading="lazy">
               <div class="branch-cap">结果 ${o.index + 1} · ${C.esc(o.label || "")} <span class="dim">#${C.esc(o.node_id || "-")}</span></div>`
            : `<div class="empty" style="min-height:80px;color:var(--red)">⚠ ${C.esc(o.error || "失败")}</div>
               <div class="branch-cap">结果 ${o.index + 1} · ${C.esc(o.label || "")}</div>`}
        </div>`;
      }).join("") : '<span class="dim">无结果</span>';
      const name = (job.image_names && job.image_names[imageId]) || imageId.slice(0, 10);
      return `<div class="job-row">
        <div class="job-row-head">
          <span class="badge ${r.status === "ok" ? "green" : "red"}">${r.status === "ok" ? "成功" : "失败"}</span>
          <strong title="${C.esc(imageId)}">${C.esc(name)}</strong>
          <span class="dim">${outs.length} 个末端${r.cache_hit ? " · 缓存命中" : ""}</span>
        </div>
        ${r.error ? `<div class="dim" style="color:var(--red)">${C.esc(r.error)}</div>` : ""}
        <div class="branch-grid">${cells}</div>
      </div>`;
    }).join("");
    box.innerHTML = rows || '<div class="empty">暂无已完成的图像</div>';
  }
})();
