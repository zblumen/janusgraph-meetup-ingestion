from . import schemas as sc
import pandas as pd
from typing import Dict
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.graph_traversal import GraphTraversalSource
from gremlin_python.process.graph_traversal import GraphTraversal
from gremlin_python.statics import long

# Type handling
VertexStagingSchemaMap = Dict[str, sc.GraphStagingSchema]

GraphElementStagingData = Dict[str, pd.DataFrame]
VertexStagingDataMap = Dict[str, GraphElementStagingData]


def instantiate_staging_data_frame(self, schema: sc.GraphStagingSchema) -> pd.DataFrame:
    return pd.DataFrame(columns=list(schema.properties.keys())).astype(schema)


class GraphIngestData:
    vertexStagingSchemaMap: Dict[str, sc.GraphStagingSchema]
    vertexStagingDataMap: Dict[str, pd.DataFrame]

    edgeStagingSchema: sc.GraphStagingSchema
    edgeStagingData: pd.DataFrame

    def __init__(self,
                 vertex_staging_schema_map: Dict[str, sc.GraphStagingSchema],
                 edge_staging_schema: sc.GraphStagingSchema
                 ):

        # create vertex schemas
        self.vertexStagingSchemaMap = vertex_staging_schema_map
        for key, value in vertex_staging_schema_map.items():
            self.vertexStagingDataMap[key] = instantiate_staging_data_frame(value)

        # create edge schema
        self.edgeStagingSchema = edge_staging_schema
        self.edgeStagingData = instantiate_staging_data_frame(edge_staging_schema)

    def staged_vertex_exists(self, tracking_id: str, label: str):
        return tracking_id in self.vertexStagingDataMap[label].index

    def get_staged_vertex(self, tracking_id: str, label: str):
        return self.vertexStagingDataMap[label].loc[tracking_id, :].se

    def stage_vertex(self, tracking_id: str, label: str, properties: Dict, check_schema=True):

        if label not in self.vertexStagingSchemaMap:
            raise RuntimeError("Cannot find the Vertex Label " + label + " in vertex schema staging schema map.")
        if check_schema:
            self.vertexStagingSchemaMap[label].check_property_fields(label, inc=False)

        self.vertexStagingDataMap[label].loc[tracking_id] = properties


class GraphIngestTracker:
    vertexTrackerSchema: sc.GraphStagingSchema
    vertexTracker: pd.DataFrame

    edgeTrackerSchema: sc.GraphStagingSchema
    edgeTracker: pd.DataFrame

    def __init__(self,
                 vertex_tracking_schema: sc.GraphStagingSchema,
                 edge_tracking_schema: sc.GraphStagingSchema
                 ):
        # create vertex schema
        self.vertexTrackerSchema = vertex_tracking_schema
        self.vertexTracker = instantiate_staging_data_frame(vertex_tracking_schema)

        # create edge schema
        self.edgeTrackerSchema = edge_tracking_schema
        self.edgeTracker = instantiate_staging_data_frame(edge_tracking_schema)

    def vertex_exists(self, tracking_id: str) -> bool:
        return tracking_id in self.vertexTracker.index

    def get_vertex(self, tracking_id: str) -> Dict:
        return self.vertexTracker[tracking_id, :].to_dict()

    def insert_vertex(self, tracking_id: str, properties: Dict, check_schema=True):
        if check_schema:
            self.vertexTrackerSchema.check_property_fields(list(properties.keys()))

        self.vertexTracker.loc[tracking_id] = properties

    def edge_exists(self, tracking_id: str) -> bool:
        return tracking_id in self.edgeTracker.index

    def get_edge(self, tracking_id: str) -> Dict:
        return self.edgeTracker[tracking_id, :].to_dict()

    def insert_edge(self, tracking_id: str, properties: Dict, check_schema=True):
        if check_schema:
            self.edgeTrackerSchema.check_property_fields(list(properties.keys()))

        self.edgeTracker.loc[tracking_id] = properties


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