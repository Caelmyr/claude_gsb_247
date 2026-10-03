"""多末端（分叉链）端到端验证：引擎 / 缓存 / 历史 / 批处理 / 保存恢复。

运行：python tests/test_multi_sinks.py
覆盖场景：
1. 两支末端（模糊 / 锐化）—— 两个都是结果，图像内容不同
2. 三支末端
3. 末端下再接节点 —— 只有真正无下游的节点是末端
4. 保存为流水线 -> 用新 id 恢复快照 -> 再跑 —— 命中同一多末端缓存，且标签正确
5. 批量跑分叉链 —— 每张图每个末端都有结果
6. 单链回归 —— 只有一个末端
7. 某一支末端节点失败 —— 不丢支、不静默，outputs 里显式标失败
"""
import io
import os
import sys
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PIL import Image, ImageDraw  # noqa: E402

from server import config, pipeline as pe  # noqa: E402
from server.algorithms import util  # noqa: E402
from server.batch import BatchManager, process_image  # noqa: E402
from server.cache import ResultCache  # noqa: E402
from server.history import HistoryManager  # noqa: E402
from server.image_store import ImageStore  # noqa: E402
from server import nodes as node_registry  # noqa: E402

FAILS = []


def check(name, cond, detail=""):
    mark = "✔" if cond else "✗"
    print(f"  [{mark}] {name}" + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


def _img():
    img = Image.new("RGB", (160, 120), (30, 60, 90))
    d = ImageDraw.Draw(img)
    for i in range(8):
        d.ellipse([i * 15, i * 8, i * 15 + 40, i * 8 + 40],
                  fill=(i * 30, 200 - i * 20, 100))
    return img


def _bytes(img):
    buf = io.BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()


def _snapshot_remap(nodes):
    """模拟「存成流水线再恢复」：节点全部换新 id，inputs 同步重映射。"""
    remap = {n["id"]: f"r{i}" for i, n in enumerate(nodes)}
    return [{"id": remap[n["id"]], "type": n["type"], "params": dict(n.get("params") or {}),
             "inputs": [remap[s] for s in (n.get("inputs") or [])]} for n in nodes]


def main():
    # 干净的 data 目录，避免历史冒烟数据干扰断言
    for d in (config.DATA_DIR,):
        if os.path.isdir(d):
            shutil.rmtree(d)
    config.ensure_dirs()

    store = ImageStore()
    cache = ResultCache()
    history = HistoryManager()

    img = _img()
    rec = store.save_upload(_bytes(img), "multi.png")
    work = util.downscale_to_max(util.ensure_rgb(img), config.MAX_DIM)

    print("== 1. 两支末端：亮度 -> [模糊, 锐化] ==")
    two = [
        {"id": "a", "type": "brightness", "params": {"amount": 30}, "inputs": []},
        {"id": "b", "type": "blur", "params": {"radius": 5}, "inputs": ["a"]},
        {"id": "c", "type": "sharpen", "params": {"amount": 80}, "inputs": ["a"]},
    ]
    check("校验通过", pe.validate(two) == [])
    check("末端识别为 [b, c]（拓扑序）", pe.sink_ids(two) == ["b", "c"], str(pe.sink_ids(two)))
    r = pe.execute(work, two)
    check("output_count == 2", r["output_count"] == 2)
    check("主输出是第一末端 b", r["output_node_id"] == "b")
    imgs = [o["image"] for o in r["outputs"]]
    diff = sum(abs(p - q) for p, q in zip(imgs[0].tobytes(), imgs[1].tobytes()))
    check("两支结果图像确实不同", diff > 0, f"diff={diff}")
    labels = [o["label"] for o in r["outputs"]]
    check("末端标签为 模糊/锐化", labels == ["模糊", "锐化"], str(labels))
    paths = [o["path"] for o in r["outputs"]]
    check("路径共享分叉点 a", all(p[0]["id"] == "a" for p in paths)
          and [p[1]["id"] for p in paths] == ["b", "c"], str(paths))

    print("== 2. 三支末端 ==")
    three = two + [{"id": "d", "type": "invert", "params": {}, "inputs": ["a"]}]
    r3 = pe.execute(work, three)
    check("output_count == 3", r3["output_count"] == 3)
    check("末端为 b/c/d", pe.sink_ids(three) == ["b", "c", "d"])

    print("== 3. 末端下再接节点 ==")
    ext = three + [{"id": "e", "type": "edges", "params": {"method": "sobel"}, "inputs": ["b"]}]
    check("b 不再是末端，末端为 c/d/e", pe.sink_ids(ext) == ["c", "d", "e"], str(pe.sink_ids(ext)))
    rext = pe.execute(work, ext)
    check("output_count == 3", rext["output_count"] == 3)

    print("== 4. process_image：缓存 + 历史逐末端保存 ==")
    res1 = process_image(store, cache, history, rec["id"], two, pipeline_name="分叉")
    check("首次计算 result_ids 有 2 个", not res1["cache_hit"] and len(res1["result_ids"]) == 2,
          str(res1["result_ids"]))
    res2 = process_image(store, cache, history, rec["id"], two, pipeline_name="分叉")
    check("二次命中缓存", res2["cache_hit"])
    check("命中后仍返回 2 个末端", len(res2["outputs"]) == 2)
    check("命中后 result_id 与首次一致",
          res1["result_ids"] == [o["result_id"] for o in res2["outputs"]])

    h = history.get(res1["history_id"])
    check("历史 output_count == 2", h["output_count"] == 2)
    check("历史 outputs 与 result_ids 一一对应",
          [o["result_id"] for o in h["outputs"]] == res1["result_ids"])
    check("历史主指针 result_id = 第一末端", h["result_id"] == res1["result_ids"][0])
    for i, rid in enumerate(res1["result_ids"]):
        check(f"末端 {i + 1} 结果文件可按 result_id 取到", cache.result_path(rid) is not None)
        entry = cache.get_entry(rid)
        check(f"末端 {i + 1} entry 携带 output_count=2/index={i}",
              entry["output_count"] == 2 and entry["output_index"] == i,
              str((entry.get("output_index"), entry.get("output_count"))))

    print("== 5. 保存为流水线 -> 恢复（id 全换）-> 再跑 ==")
    restored = _snapshot_remap(two)
    res3 = process_image(store, cache, history, rec["id"], restored, pipeline_name="分叉恢复")
    check("恢复后重跑仍命中缓存（canonical_key 与 id 无关）", res3["cache_hit"])
    check("恢复后末端标签用当前图重算为 r1/r2",
          [o["node_id"] for o in res3["outputs"]] == ["r1", "r2"],
          str([o["node_id"] for o in res3["outputs"]]))

    print("== 6. 批处理跑分叉链 ==")
    bm = BatchManager(store, cache, history)
    job = bm.enqueue(two, [rec["id"]], pipeline_name="批量分叉")
    import time
    for _ in range(60):
        time.sleep(0.1)
        j = bm.get_job(job["id"])
        if j["status"] in ("done", "partial", "cancelled"):
            break
    j = bm.get_job(job["id"])
    check("批量任务完成", j["status"] == "done", j["status"])
    rr = j["results"][rec["id"]]
    check("每张图 output_count == 2", rr["output_count"] == 2, str(rr.keys()))
    check("每张图 2 个末端 result_id", len(rr["result_ids"]) == 2)

    print("== 7. 单链回归 ==")
    chain = [
        {"id": "x", "type": "brightness", "params": {"amount": 10}, "inputs": []},
        {"id": "y", "type": "blur", "params": {"radius": 1}, "inputs": ["x"]},
    ]
    check("单链只有一个末端", pe.sink_ids(chain) == ["y"])
    rc = process_image(store, cache, history, rec["id"], chain, pipeline_name="单链")
    check("单链 outputs 长度为 1", len(rc["outputs"]) == 1)
    check("单链主 result_id 可用", cache.result_path(rc["result_id"]) is not None)

    print("== 8. 某一支失败：不丢支、不静默 ==")
    # 注册一个必定抛异常的临时节点，模拟「末端下再跑时某一支运行期失败」
    def _boom(image, params, meta):
        raise RuntimeError("该支故意失败")
    node_registry.NODES["__boom__"] = {
        "type": "__boom__", "label": "故意失败", "category": "滤镜",
        "desc": "", "schema": [], "defaults": {}, "min_inputs": 0, "max_inputs": 1,
        "handler": _boom,
    }
    try:
        bad = [
            {"id": "a", "type": "brightness", "params": {"amount": 30}, "inputs": []},
            {"id": "b", "type": "blur", "params": {"radius": 5}, "inputs": ["a"]},
            {"id": "c", "type": "__boom__", "params": {}, "inputs": ["a"]},
        ]
        rb = pe.execute(work, bad)
        check("失败支仍在 outputs 中", rb["output_count"] == 2)
        oks = {o["node_id"]: o["ok"] for o in rb["outputs"]}
        check("b 成功 / c 失败", oks.get("b") is True and oks.get("c") is False, str(oks))
        check("失败支带 error 文本", bool(rb["outputs"][1]["error"]))
        resb = process_image(store, cache, history, rec["id"], bad, pipeline_name="分叉含失败")
        check("含失败的运行不写整组缓存（二次不命中）",
              process_image(store, cache, history, rec["id"], bad)["cache_hit"] is False)
        check("成功支仍有 result_id 可展示",
              resb["outputs"][0]["result_id"] is not None)
        check("失败支 result_id 为 None 但保留在列表里",
              resb["outputs"][1]["result_id"] is None and resb["outputs"][1]["ok"] is False)
    finally:
        node_registry.NODES.pop("__boom__", None)

    print()
    if FAILS:
        print(f"存在 {len(FAILS)} 项失败：")
        for f in FAILS:
            print("  -", f)
        sys.exit(1)
    print("多末端全部断言通过 ✔")


if __name__ == "__main__":
    main()
