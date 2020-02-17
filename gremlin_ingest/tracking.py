import pandas as pd
from typing import Dict, List
from enum import Enum


class GraphPrepDataTypeEnum(Enum):
    OBJECT = "object"
    INT64 = "int64"
    FLOAT64 = "float64"
    BOOL = "bool"
    DATETIME64 = "datetime64"


class GraphStagingSchema:
    properties: Dict[str, GraphPrepDataTypeEnum]

    def __init__(self, schema: Dict[str, GraphPrepDataTypeEnum]):
        self.properties = schema

    def check_property_fields(self, fields: List[str], exc: bool = True, inc: bool = True):
        keys = list(self.properties.keys())
        if exc:
            for field in fields:
                if field not in keys:
                    raise RuntimeError(field + " is not in the public graph schema for this element")
        if inc:
            for key in keys:
                if key not in fields:
                    raise RuntimeError(key + " is a public property but isn't present in the provided row fields")


# Type handling
VertexStagingSchemaMap = Dict[str, GraphStagingSchema]

GraphElementStagingData = Dict[str, pd.DataFrame]
VertexStagingDataMap = Dict[str, GraphElementStagingData]


def instantiate_staging_data_frame(schema: GraphStagingSchema) -> pd.DataFrame:
    return pd.DataFrame(columns=list(schema.properties.keys())).astype(schema)


class GraphIngestTracker:
    vertexTrackerSchema: GraphStagingSchema
    vertexTracker: pd.DataFrame

    edgeTrackerSchema: GraphStagingSchema
    edgeTracker: pd.DataFrame

    def __init__(self,
                 vertex_tracking_schema: GraphStagingSchema,
                 edge_tracking_schema: GraphStagingSchema
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

    def insert_vertex_tracking(self, tracking_id: str, properties: Dict, check_schema=True):
        if check_schema:
            self.vertexTrackerSchema.check_property_fields(list(properties.keys()))

        self.vertexTracker.loc[tracking_id] = properties

    def edge_exists(self, tracking_id: str) -> bool:
        return tracking_id in self.edgeTracker.index

    def get_edge(self, tracking_id: str) -> Dict:
        return self.edgeTracker[tracking_id, :].to_dict()

    def insert_edge_tracking(self, tracking_id: str, properties: Dict, check_schema=True):
        if check_schema:
            self.edgeTrackerSchema.check_property_fields(list(properties.keys()))

        self.edgeTracker.loc[tracking_id] = properties
