from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.graph_traversal import GraphTraversalSource
from gremlin_python.process.graph_traversal import GraphTraversal
from gremlin_python.statics import long
from typing import Dict


def get_connection() -> GraphTraversalSource:
    gremlin_service: str = input("Enter gremlin server (complete url)")
    graph_name: str = input("Enter traversal source name")
    conn = DriverRemoteConnection(gremlin_service, graph_name)
    return traversal().withRemote(conn)


def insert_vertex(g: GraphTraversalSource, label: str, properties: Dict) -> int:
    vertex: GraphTraversal = g.addV(label)
    for k, v in properties.items():
        vertex.property(k, v)
    return vertex.id().next()


def insert_edge(g: GraphTraversalSource, from_gremlin_id: str, to_gremlin_id: str, label: str, properties: Dict):
    edge: GraphTraversal = g.V(from_gremlin_id).to(g.V(to_gremlin_id))
    for k, v in properties.items():
        edge.property(k, v)
    edge.iterate()
