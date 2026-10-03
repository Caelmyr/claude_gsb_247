"""流水线引擎：DAG 校验、拓扑排序、执行与结果组装。

难点之一「流水线引擎设计」的核心实现：

- 节点用 inputs 表达依赖边（单输入链式，支持扇出）。执行前做完整校验：
  类型存在性、id 唯一、输入引用存在、输入数量在 min/max 内、无环。
- Kahn 拓扑排序决定执行顺序；每个节点消费其唯一上游节点的输出「数据包」，
  数据包 = 图像 + meta（meta 携带关键点/检测框/分割区域等非图像数据，
  供检测->画框、分割->统计这类下游节点复用）。
- 每节点执行包裹 try/except，错误记录到该节点，前端可定位失败点。
- 一条链允许扇出多个末端（sink）：所有「无下游消费者」的节点都是结果，
  执行后全部收集进 outputs，绝不静默丢弃；单链场景 outputs 只有一项。
- canonical_key() 生成与布局/命名无关的确定性哈希（连边拓扑也参与），供结果缓存使用。
"""
import json

from . import nodes as node_registry


class Packet:
    """节点间传递的数据包：图像 + 附加元数据。"""

    def __init__(self, image=None, meta=None):
        self.image = image
        self.meta = meta if meta is not None else {}


def _merge_params(node):
    spec = node_registry.get_node(node.get("type"))
    defaults = dict(spec["defaults"]) if spec else {}
    merged = dict(defaults)
    merged.update(node.get("params") or {})
    return merged


def validate(nodes):
    """返回错误列表；空列表表示合法。"""
    errors = []
    ids = set()
    for n in nodes:
        nid = n.get("id")
        if not nid:
            errors.append("存在缺少 id 的节点")
            continue
        if nid in ids:
            errors.append(f"节点 id 重复：{nid}")
        ids.add(nid)
        spec = node_registry.get_node(n.get("type"))
        if spec is None:
            errors.append(f"未知节点类型：{n.get('type')}")
            continue
        ni = len(n.get("inputs") or [])
        if ni < spec["min_inputs"] or ni > spec["max_inputs"]:
            errors.append(f"节点 {nid} 输入数量 {ni} 超出允许范围 "
                          f"[{spec['min_inputs']}, {spec['max_inputs']}]")

    for n in nodes:
        for inp in (n.get("inputs") or []):
            if inp not in ids:
                errors.append(f"节点 {n.get('id')} 引用了不存在的输入 {inp}")

    if not errors:
        ordered, leftover = _topo(nodes)
        if leftover:
            errors.append("流水线存在环，无法执行")
    return errors


def _topo(nodes):
    """Kahn 拓扑排序。返回 (ordered_ids, remaining_ids)。"""
    indeg = {}
    children = {}
    by_id = {n["id"]: n for n in nodes}
    for n in nodes:
        indeg[n["id"]] = len(n.get("inputs") or [])
        children.setdefault(n["id"], [])
    for n in nodes:
        for inp in (n.get("inputs") or []):
            children.setdefault(inp, []).append(n["id"])

    queue = [nid for nid, d in indeg.items() if d == 0]
    ordered = []
    while queue:
        nid = queue.pop(0)
        ordered.append(nid)
        for c in children.get(nid, []):
            indeg[c] -= 1
            if indeg[c] == 0:
                queue.append(c)
    remaining = [nid for nid, d in indeg.items() if d > 0]
    return ordered, remaining


def topological_order(nodes):
    ordered, _ = _topo(nodes)
    return ordered


def canonical_key(nodes):
    """生成与布局/id 命名无关、但与连边拓扑一致的确定性流水线指纹。

    节点按拓扑序编号，inputs 用「拓扑序号」表示，因此保存为流水线再恢复
    （节点 id 全部重排）仍命中同一缓存；而分叉结构不同的链不会误命中。
    """
    ordered, _ = _topo(nodes)
    by_id = {n["id"]: n for n in nodes}
    topo_idx = {nid: i for i, nid in enumerate(ordered)}
    seq = []
    for nid in ordered:
        n = by_id[nid]
        input_idx = [topo_idx[s] for s in (n.get("inputs") or [])]
        seq.append({"type": n["type"], "params": _merge_params(n), "inputs": input_idx})
    return json.dumps(seq, sort_keys=True, separators=(",", ":"))


def sink_ids(nodes):
    """返回全部末端节点 id（无下游消费者），按拓扑序排列。

    注意：必须在已通过 validate（无环、引用有效）的图上调用。
    """
    ordered, _ = _topo(nodes)
    consumers = set()
    for n in nodes:
        for inp in (n.get("inputs") or []):
            consumers.add(inp)
    return [nid for nid in ordered if nid not in consumers]


def _sink_paths(nodes, ordered, sinks):
    """为每个末端生成「从源到末端」的路径标签。

    当前所有节点都是单输入，每条末端路径唯一（沿 inputs 回溯到入度为 0
    的根节点，再反转）。返回 {sink_id: [{id, type}, ...]}，分叉链的路径
    天然在分叉点之前共享前缀，前端可用「分叉节点 → 末端」区分各支。
    """
    by_id = {n["id"]: n for n in nodes}
    roots = {nid for nid in ordered if not (by_id[nid].get("inputs") or [])}
    paths = {}
    for sink in sinks:
        chain = []
        cur = sink
        seen = set()
        while cur and cur not in seen:
            seen.add(cur)
            chain.append({"id": cur, "type": by_id[cur]["type"]})
            ins = by_id[cur].get("inputs") or []
            cur = ins[0] if ins else None
        chain.reverse()
        # 根不唯一（多个无输入节点）时，路径仍以该支自己的根开头，保持可追溯
        paths[sink] = chain if chain else [{"id": sink, "type": by_id[sink]["type"]}]
    return paths


def describe_sinks(nodes):
    """返回全部末端的可展示信息（不含图像）：[{index, node_id, type, label, path}]。

    缓存命中时节点 id 已与当时不同（保存/恢复会重排 id），用当前图重新计算
    这部分标签；缓存里只复用结果文件与 meta。空链返回单个 source 占位。
    """
    ordered, _ = _topo(nodes)
    sinks = sink_ids(nodes)
    if not sinks:
        return [{"index": 0, "node_id": None, "type": "source",
                 "label": "原图", "path": []}]
    paths = _sink_paths(nodes, ordered, sinks)
    by_id = {n["id"]: n for n in nodes}
    infos = []
    for i, nid in enumerate(sinks):
        infos.append({
            "index": i, "node_id": nid, "type": by_id[nid]["type"],
            "label": node_registry.get_node(by_id[nid]["type"])["label"],
            "path": paths[nid],
        })
    return infos


def execute(image, nodes, source_meta=None):
    """在给定图像上执行流水线。

    所有无下游消费者的节点都算「末端结果」，全部收集到 outputs：
      outputs        - [{index, node_id, type, label, path, ok, error,
                         image, meta}]，顺序 = 末端的拓扑序
      output_count   - 末端总数
      image/meta     - 主输出（第一个末端）的图像与 meta；无节点时为源图
      output_node_id - 主输出节点（无节点时为 None）
      node_results   - [{node_id, type, ok, error}] 逐节点状态
      error          - 顶层错误（校验失败等）
    """
    errors = validate(nodes)
    if errors:
        return {"image": image, "meta": source_meta or {}, "node_results": [],
                "outputs": [], "output_count": 0,
                "output_node_id": None, "error": "; ".join(errors)}

    ordered, _ = _topo(nodes)
    by_id = {n["id"]: n for n in nodes}
    packets = {"__source__": Packet(image, source_meta or {})}
    node_results = []

    for nid in ordered:
        node = by_id[nid]
        spec = node_registry.get_node(node["type"])
        inputs = node.get("inputs") or []
        input_packet = packets.get(inputs[0], packets["__source__"]) if inputs else packets["__source__"]
        params = _merge_params(node)
        try:
            out_img, out_meta = spec["handler"](input_packet.image, params, input_packet.meta)
            packets[nid] = Packet(out_img, out_meta)
            node_results.append({"node_id": nid, "type": node["type"], "ok": True, "error": None})
        except Exception as exc:  # noqa: BLE001 —— 记录但继续，让前端能看到失败节点与其他支结果
            packets[nid] = Packet(input_packet.image, input_packet.meta)
            node_results.append({"node_id": nid, "type": node["type"], "ok": False,
                                 "error": f"{type(exc).__name__}: {exc}"})

    result_by_id = {r["node_id"]: r for r in node_results}
    outputs = []
    for info in describe_sinks(nodes):
        nid = info["node_id"]
        if nid is None:
            # 空链：输出源图
            pkt = packets["__source__"]
            outputs.append({**info, "ok": True, "error": None,
                            "image": pkt.image, "meta": pkt.meta})
            continue
        nr = result_by_id[nid]
        pkt = packets[nid]
        outputs.append({**info, "ok": nr["ok"], "error": nr["error"],
                        "image": pkt.image if nr["ok"] else None,
                        "meta": pkt.meta if nr["ok"] else {}})

    primary = outputs[0]
    return {
        "image": primary["image"], "meta": primary["meta"],
        "node_results": node_results, "outputs": outputs,
        "output_count": len(outputs),
        "output_node_id": primary["node_id"], "error": None,
    }
