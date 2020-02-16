from typing import Dict
from enum import Enum


# corresponds to pandas types - object covers string, dicts, arrays, etc.
class GraphPrepDataTypes(Enum):
    OBJECT = "object"
    INT64 = "int64"
    FLOAT64 = "float64"
    BOOL = "bool"
    DATETIME64 = "datetime64"


# Allowed Data types for vertex map prep
graph_prep_data_types = GraphPrepDataTypes()

# Type handling
GraphElementStagingSchema = Dict[str, graph_prep_data_types]


class GraphStagingSchema:
    public: GraphElementStagingSchema
    private: GraphElementStagingSchema
    constants: GraphElementStagingSchema

    def __init__(self,
                 public: GraphElementStagingSchema,
                 private: GraphElementStagingSchema = None,
                 constants: Dict = None
                 ):

        self.public = public
        self.private = private
        self.constants = constants

     def check_public_fields(self,fields:str):
        keys = list(self.public.keys())
        for field in fields:
            if field not in keys:
                raise RuntimeError(field + " is not in the public graph schema for this element")
        for key in keys:
            if key not in fields:
                raise RuntimeError(key + " is a public property but isn't present in the provided row fields")
