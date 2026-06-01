def serialize_graph(graph):
    return {"nodes": graph.get("nodes", []), "edges": graph.get("edges", [])}
