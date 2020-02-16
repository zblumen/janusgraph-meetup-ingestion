from . import schemas as sc
import pandas as pd
from typing import Dict

# Type handling
VertexStagingSchemaMap = Dict[str, sc.GraphStagingSchema]

GraphElementStagingData = Dict[str, pd.DataFrame]
VertexStagingDataMap = Dict[str, GraphElementStagingData]


def __add_default_properties_to_vertex_staging_schema__(
        vertex_staging_schema: sc.GraphStagingSchema) -> sc.GraphStagingSchema:
    
    vertex_staging_schema.private["VertexCreationDate"] = sc.graph_prep_data_types.DATETIME64
    vertex_staging_schema.public["_OutEdges"] = sc.graph_prep_data_types.OBJECT
    vertex_staging_schema.public["_InEdges"] = sc.graph_prep_data_types.OBJECT

    
    return vertex_staging_schema


def __add_default_properties_to_edge_staging_schema__(
        edge_staging_schema: sc.GraphStagingSchema, ) -> sc.GraphStagingSchema:
    
    edge_staging_schema.private["EdgeCreationDate"] = sc.graph_prep_data_types.DATETIME64
    edge_staging_schema.public["_GremlinFromId"] = sc.graph_prep_data_types.OBJECT
    edge_staging_schema.public["_GremlinToId"] = sc.graph_prep_data_types.OBJECT
    return edge_staging_schema


class GraphIngestData:
    def __init__(self,
                 vertex_staging_schema_map: VertexStagingSchemaMap,
                 edge_staging_schema: sc.GraphStagingSchema,
                 universal_ingest_tags: Dict[str, str] = None
                 ):

        self.__universal_ingest_tags__ = universal_ingest_tags

        # create vertex schemas
        self.vertex_staging_data_map = {}
        self.vertex_staging_schema_map = {}
        for key, value in vertex_staging_schema_map.items():
            self.vertex_staging_data_map[key] = self.__instantiate_staging_data_frame__(
                __add_default_properties_to_vertex_staging_schema__(value, universal_ingest_tags)
            )

        # create edge schema
        self.edge_staging_data = self.__instantiate_staging_data_frame__(
            __add_default_properties_to_edge_staging_schema__(edge_staging_schema, universal_ingest_tags)
        )

    def stage_vertex(self, label:str,properties:Dict,check_schema=True):
        True
    ## def __check_properties__(self,):
    def __instantiate_staging_data_frame__(self, schema: sc.GraphStagingSchema) -> pd.DataFrame:
        if self.__universal_ingest_tags__ is not None:
            for key in self.__universal_ingest_tags__:
                schema[key] = sc.graph_prep_data_types.OBJECT

        return pd.DataFrame(columns=list(schema.keys())).astype(schema)
