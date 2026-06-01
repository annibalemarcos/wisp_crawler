from __future__ import annotations
import time
from utils.hashing import stable_id

class GraphBuilder:
    def __init__(self):
        self.nodes = {}
        self.edges = {}

    def add_node(self, url: str, **meta):
        nid = stable_id(url)
        node = self.nodes.get(nid, {"id": nid, "url": url})
        node.update({k:v for k,v in meta.items() if v is not None})
        node.setdefault("discovered_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        self.nodes[nid] = node
        return node

    def add_edge(self, src: str, dst: str, source="crawl", relationship="links_to"):
        eid = stable_id(src, dst, source, relationship)
        edge = {"id": eid, "from": stable_id(src), "to": stable_id(dst), "source": source, "relationship": relationship, "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        self.edges[eid] = edge
        return edge

    def as_dict(self):
        return {"nodes": list(self.nodes.values()), "edges": list(self.edges.values())}
