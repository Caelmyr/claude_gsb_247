"""生成测试图像并冒烟验证核心链路（存储/引擎/算法/缓存/批处理）。

运行：python tests/smoke.py
会在 data/ 下生成几张测试图，然后走通：上传 -> 流水线 -> 特征/检测/分割/风格
-> 缓存 -> 历史 -> 批处理，打印每步摘要。不做 HTTP 层。
"""
import io
import random
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PIL import Image, ImageDraw, ImageFilter  # noqa: E402

from server import config, pipeline as pipeline_engine  # noqa: E402
from server.algorithms import detection, features, segmentation, style  # noqa: E402
from server.batch import BatchManager, process_image  # noqa: E402
from server.cache import ResultCache  # noqa: E402
from server.history import HistoryManager  # noqa: E402
from server.image_store import ImageStore  # noqa: E402


def _gradient(w=320, h=240):
    img = Image.new("RGB", (w, h))
    px = img.load()
    for y in range(h):
        for x in range(w):
            px[x, y] = (x * 255 // w, y * 255 // h, (x + y) * 255 // (w + h))
    return img


def _shapes(w=320, h=240):
    img = Image.new("RGB", (w, h), (240, 240, 240))
    d = ImageDraw.Draw(img)
    for i in range(12):
        x, y = random.randrange(w), random.randrange(h)
        r = random.randrange(15, 50)
        color = (random.randrange(256), random.randrange(256), random.randrange(256))
        if i % 3 == 0:
            d.ellipse([x, y, x + r, y + r], fill=color)
        elif i % 3 == 1:
            d.rectangle([x, y, x + r, y + r], fill=color)
        else:
            d.polygon([(x, y), (x + r, y), (x + r // 2, y + r)], fill=color)
    return img


def _texture(w=320, h=240):
    img = _gradient(w, h).filter(ImageFilter.EMBOSS)
    return img


def _to_bytes(img):
    buf = io.BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()


def main():
    config.ensure_dirs()
    image_store = ImageStore()
    cache = ResultCache()
    history = HistoryManager()

    print("== 生成测试图 ==")
    fixtures = {
        "gradient.png": _gradient(),
        "shapes.png": _shapes(),
        "texture.png": _texture(),
    }
    ids = {}
    for name, img in fixtures.items():
        rec = image_store.save_upload(_to_bytes(img), name)
        ids[name] = rec["id"]
        print(f"  上传 {name} -> id={rec['id'][:12]}..  {rec['width']}x{rec['height']}")

    print("\n== 流水线引擎 ==")
    nodes = [
        {"id": "n1", "type": "brightness", "params": {"amount": 25}, "inputs": []},
        {"id": "n2", "type": "blur", "params": {"radius": 2}, "inputs": ["n1"]},
        {"id": "n3", "type": "edges", "params": {"method": "sobel"}, "inputs": ["n2"]},
    ]
    errors = pipeline_engine.validate(nodes)
    print(f"  校验错误: {errors}")
    rec, img = _load(image_store, ids["shapes.png"])
    from server.algorithms import util
    work = util.downscale_to_max(img, config.MAX_DIM)
    result = pipeline_engine.execute(work, nodes)
    print(f"  执行结果: error={result['error']}  输出节点={result['output_node_id']}  节点状态={[r['ok'] for r in result['node_results']]}")
    print(f"  规范化键: {pipeline_engine.canonical_key(nodes)[:40]}...")

    print("\n== 多末端（分叉）：每个 sink 都是一路独立结果，一个都不能丢 ==")
    fork = [
        {"id": "f1", "type": "brightness", "params": {"amount": 25}, "inputs": []},
        {"id": "f2", "type": "blur", "params": {"radius": 3}, "inputs": ["f1"]},
        {"id": "f3", "type": "sharpen", "params": {"amount": 60}, "inputs": ["f1"]},
        {"id": "f4", "type": "invert", "params": {}, "inputs": ["f3"]},
    ]
    sinks = pipeline_engine.sink_nodes(fork)
    assert sinks == ["f2", "f4"], sinks
    fresult = pipeline_engine.execute(work, fork)
    assert [o["node_id"] for o in fresult["outputs"]] == ["f2", "f4"]
    assert fresult["output_node_id"] == "f2"  # 主输出 = 拓扑序第一个末端
    fres = process_image(image_store, cache, history, ids["shapes.png"], fork, pipeline_name="分叉冒烟")
    assert fres["error"] is None
    assert len(fres["outputs"]) == 2, "分叉的两个末端都应产出结果"
    sink_ids_seen = {o["node_id"] for o in fres["outputs"]}
    assert sink_ids_seen == {"f2", "f4"}, sink_ids_seen
    assert all(o["result_id"] for o in fres["outputs"]), "每路末端都要有独立 result_id"
    fentry = history.get(fres["history_id"])
    assert len(fentry["outputs"]) == 2, "历史记录必须保存全部末端"
    print(f"  末端节点: {sinks} -> 每路各一个 result_id: "
          f"{[(o['node_id'], o['result_id'][:8]) for o in fres['outputs']]}")

    print("\n== process_image（含缓存/历史）==")
    res1 = process_image(image_store, cache, history, ids["shapes.png"], nodes, pipeline_name="冒烟")
    res2 = process_image(image_store, cache, history, ids["shapes.png"], nodes, pipeline_name="冒烟")
    print(f"  首次: result={res1['result_id'][:12]}.. cache_hit={res1['cache_hit']} error={res1['error']}")
    print(f"  再次: result={res2['result_id'][:12]}.. cache_hit={res2['cache_hit']} (应命中缓存)")
    print(f"  历史条数: {len(history.list())}")

    print("\n== 特征提取 ==")
    r = features.extract_keypoints(work, {"method": "sift", "max_points": 80})
    print(f"  SIFT 关键点: {r['count']}  描述子维度={r['descriptor_dim']}")
    r = features.extract_keypoints(work, {"method": "orb", "max_points": 80})
    print(f"  ORB 关键点: {r['count']}  描述子维度={r['descriptor_dim']}")

    print("\n== 目标检测 ==")
    r = detection.detect(work, {"method": "saliency", "max_boxes": 20})
    print(f"  检测框: {r['count']}  阈值={r['threshold_used']}  标签样例={[b['label'] for b in r['boxes'][:4]]}")

    print("\n== 图像分割 ==")
    r = segmentation.segment(work, {"method": "color", "colors": 5})
    print(f"  区域数: {r['region_count']}  覆盖率={r['coverage']}")

    print("\n== 风格迁移 ==")
    for s in ("oil", "sketch", "cyber"):
        r = style.apply(work, {"style": s, "strength": 100})
        print(f"  {s}: {r['description']}")

    print("\n== 批处理 ==")
    batch = BatchManager(image_store, cache, history)
    job = batch.enqueue(nodes, [ids["gradient.png"], ids["texture.png"]], pipeline_name="批量冒烟")
    import time
    for _ in range(60):
        time.sleep(0.1)
        j = batch.get_job(job["id"])
        if j["status"] in ("done", "partial", "cancelled"):
            break
    j = batch.get_job(job["id"])
    print(f"  任务状态: {j['status']}  完成 {j['done']}/{j['total']}")
    for iid, r in j["results"].items():
        print(f"    {iid[:12]}.. -> {r['status']} cache_hit={r['cache_hit']}")

    print("\n== 批处理分叉链：每张图都应带全两路末端结果 ==")
    fjob = batch.enqueue(fork, [ids["gradient.png"], ids["texture.png"]], pipeline_name="批量分叉冒烟")
    for _ in range(60):
        time.sleep(0.1)
        fj = batch.get_job(fjob["id"])
        if fj["status"] in ("done", "partial", "cancelled"):
            break
    fj = batch.get_job(fjob["id"])
    print(f"  任务状态: {fj['status']}  完成 {fj['done']}/{fj['total']}")
    for iid, r in fj["results"].items():
        out_ids = {o["node_id"] for o in r.get("outputs", [])}
        assert out_ids == {"f2", "f4"}, (iid, out_ids)
        print(f"    {iid[:12]}.. -> {r['status']} 末端 {sorted(out_ids)} "
              f"结果 {[o['result_id'][:8] for o in r['outputs']]}")

    print("\n== 一致性检查 ==")
    issues = image_store.reconcile()
    print(f"  孤儿元数据: {issues['orphan_meta']}  孤儿文件: {issues['orphan_files']}")

    print("\n全部冒烟通过 ✔")


def _load(image_store, image_id):
    from PIL import Image as _I
    return image_store.get(image_id), _I.open(image_store.file_path(image_id))


if __name__ == "__main__":
    main()
