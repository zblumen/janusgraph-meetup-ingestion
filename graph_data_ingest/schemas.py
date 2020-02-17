from typing import Dict, List
from enum import Enum


## To do typing should ho[pefully be nonQuated....eventually
# corresponds to pandas types - object covers string, dicts, arrays, etc.
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
