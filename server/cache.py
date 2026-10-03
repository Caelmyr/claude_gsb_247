"""结果缓存与结果文件管理。

难点之一「结果缓存」：同一个（图像 + 流水线）组合不重复计算。

- 缓存键 = sha256(图像内容哈希 + 流水线规范化 JSON)，命中直接复用结果文件。
- 结果图落盘到 data/results/<result_id>.png（原子写），cache.json 记录键->结果映射。
- LRU 淘汰：超过条目数或字节数上限时，按最后访问时间踢掉最久未用的结果，
  同步删除其文件，保持 JSON 与文件一致。
- 特征/检测/分割/风格等单图接口也统一走这里，天然获得缓存能力。
"""
import hashlib
import json
import os
import time
import uuid

from PIL import Image

from . import config
from .storage import JsonStore, atomic_write_bytes, now_iso
from .algorithms import util


def make_key(*parts):
    """由若干字符串片段生成确定性缓存键。"""
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()


class ResultCache:
    def __init__(self):
        self.store = JsonStore(config.CACHE_JSON, {})

    # ------------------------------------------------------------------ 读
    def _entry_outputs(self, entry):
        """把一条缓存条目展开为「每个末端一个描述符」。

        老式单输出条目（特征/检测等单图接口写入，无 outputs 字段）合成单元素；
        多末端条目则按 outputs 逐项返回。
        """
        outs = entry.get("outputs")
        if not outs:
            return [{
                "result_id": entry.get("result_id"),
                "file": entry.get("file", ""),
                "width": entry.get("width"),
                "height": entry.get("height"),
                "size_bytes": entry.get("size_bytes"),
                "meta": entry.get("meta", {}),
                "node_id": None, "type": None, "label": None, "path": [],
                "index": 0,
            }]
        return outs

    def _flatten(self):
        """全部条目 × 每个末端 -> 扁平描述符列表（含条目级公共字段）。"""
        flat = []
        for entry in self.store.read().values():
            total = len(self._entry_outputs(entry))
            for out in self._entry_outputs(entry):
                flat.append({
                    **out,
                    "key": entry.get("key"),
                    "created_at": entry.get("created_at"),
                    "last_access": entry.get("last_access"),
                    "output_index": out.get("index", 0),
                    "output_count": total,
                })
        return flat

    def get(self, key):
        entry = self.store.read().get(key)
        if not entry:
            return None
        path = os.path.join(config.RESULTS_DIR, entry.get("file", ""))
        if not os.path.exists(path):
            return None
        self._touch(key)
        return entry.get("result_id")

    def get_outputs(self, key):
        """多末端命中时返回全部末端描述符；任一结果文件缺失视为未命中。"""
        entry = self.store.read().get(key)
        if not entry:
            return None
        outs = self._entry_outputs(entry)
        for out in outs:
            if not os.path.exists(os.path.join(config.RESULTS_DIR, out.get("file", ""))):
                return None
        self._touch(key)
        return outs

    def _touch(self, key):
        def _upd(doc):
            doc = dict(doc)
            if key in doc:
                e = dict(doc[key])
                e["last_access"] = time.time()
                doc[key] = e
            return doc
        self.store.update(_upd)

    def get_entry(self, result_id):
        for out in self._flatten():
            if out.get("result_id") == result_id:
                return out
        return None

    def result_path(self, result_id):
        entry = self.get_entry(result_id)
        if not entry:
            return None
        p = os.path.join(config.RESULTS_DIR, entry.get("file", ""))
        return p if os.path.exists(p) else None

    def result_image(self, result_id):
        p = self.result_path(result_id)
        if not p:
            return None
        try:
            return Image.open(p)
        except Exception:
            return None

    def list_results(self):
        """按创建时间倒序返回结果描述符列表（多末端的每次运行占多项）。"""
        entries = self._flatten()
        entries.sort(key=lambda e: e.get("created_at", ""), reverse=True)
        return entries

    # ------------------------------------------------------------------ 写
    def _write_image_file(self, image):
        """落盘一张结果图，返回 (result_id, file_name, size, width, height)。"""
        result_id = uuid.uuid4().hex
        file_name = result_id + ".png"
        dest = os.path.join(config.RESULTS_DIR, file_name)

        rgb = util.ensure_rgb(image)
        # 原子写：先写临时文件再 rename
        tmp = dest + ".tmp"
        rgb.save(tmp, "PNG", optimize=True)
        os.replace(tmp, dest)
        return result_id, file_name, os.path.getsize(dest), rgb.size[0], rgb.size[1]

    def put(self, key, image, meta=None):
        """保存单张结果图并登记缓存，返回 result_id。"""
        result_id, file_name, size, width, height = self._write_image_file(image)

        entry = {
            "result_id": result_id,
            "key": key,
            "file": file_name,
            "size_bytes": size,
            "width": width,
            "height": height,
            "meta": meta or {},
            "created_at": now_iso(),
            "last_access": time.time(),
        }

        def _upd(doc):
            doc = dict(doc)
            doc[key] = entry
            return doc

        self.store.update(_upd)
        self.evict_if_needed()
        return result_id

    def put_many(self, key, items):
        """一次运行产生多个末端结果：逐项落盘，登记在同一条缓存条目下。

        items: [{image, meta, node_id, type, label, path, index, ok}]
        返回 outputs 描述符列表（含 result_id/file/尺寸）。整条目录取第一个
        末端作为主结果（result_id/file 等字段保持单输出时代的语义）。
        """
        now = now_iso()
        outputs = []
        total_bytes = 0
        for item in items:
            rid, file_name, size, width, height = self._write_image_file(item["image"])
            total_bytes += size
            outputs.append({
                "result_id": rid,
                "file": file_name,
                "width": width,
                "height": height,
                "size_bytes": size,
                "meta": item.get("meta") or {},
                "node_id": item.get("node_id"),
                "type": item.get("type"),
                "label": item.get("label"),
                "path": item.get("path") or [],
                "index": item.get("index", len(outputs)),
                "ok": bool(item.get("ok", True)),
            })

        primary = outputs[0]
        entry = {
            "result_id": primary["result_id"],
            "key": key,
            "file": primary["file"],
            "size_bytes": total_bytes,
            "width": primary["width"],
            "height": primary["height"],
            "meta": primary["meta"],
            "created_at": now,
            "last_access": time.time(),
            "outputs": outputs,
        }

        def _upd(doc):
            doc = dict(doc)
            doc[key] = entry
            return doc

        self.store.update(_upd)
        self.evict_if_needed()
        return outputs

    # ------------------------------------------------------------------ 淘汰
    def evict_if_needed(self):
        entries = self.store.read()
        if not entries:
            return 0
        total_bytes = sum(e.get("size_bytes", 0) for e in entries.values())
        count = len(entries)
        if count <= config.CACHE_MAX_ENTRIES and total_bytes <= config.CACHE_MAX_BYTES:
            return 0

        # 按最后访问时间升序，优先淘汰最久未用；多末端条目整体淘汰
        order = sorted(entries.items(), key=lambda kv: kv[1].get("last_access", 0))
        removed = 0
        while order and (len(entries) > config.CACHE_MAX_ENTRIES
                         or total_bytes > config.CACHE_MAX_BYTES):
            key, entry = order.pop(0)
            for out in self._entry_outputs(entry):
                path = os.path.join(config.RESULTS_DIR, out.get("file", ""))
                try:
                    if os.path.exists(path):
                        os.unlink(path)
                except OSError:
                    pass
            entries.pop(key, None)
            total_bytes -= entry.get("size_bytes", 0)
            removed += 1
        self.store.write(entries)
        return removed

    def delete_result(self, result_id):
        """按 result_id 删除结果（供历史删除联动）。

        多末端条目里任一分支被删时，整条运行（同一次分叉链的所有末端）一起删，
        避免历史记录里其余分支变成死链。
        """
        entries = self.store.read()
        target_key = None
        for key, entry in entries.items():
            ids = {o.get("result_id") for o in self._entry_outputs(entry)}
            if result_id in ids:
                target_key = key
                break
        if target_key is None:
            return False
        entry = entries[target_key]
        for out in self._entry_outputs(entry):
            path = os.path.join(config.RESULTS_DIR, out.get("file", ""))
            try:
                if os.path.exists(path):
                    os.unlink(path)
            except OSError:
                pass

        def _upd(doc):
            doc = dict(doc)
            doc.pop(target_key, None)
            return doc
        self.store.update(_upd)
        return True
