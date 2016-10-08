from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import class_mapper
from datetime import datetime

from .utils import verify_dict

data_types = {
    "String": str,
    "Integer": int,
    "Float": float,
    "DateTime": datetime,
    "BigInteger": long,
    "Boolean": bool
}

class NestedModelProcessor:
    def __init__(self, key, model, foreign_key_list):
        self.key = key
        self.model = model
        self.fklist = foreign_key_list
        self.mp = ModelProcessor(model)

    def insert_bson_entry(self, bdata, session):
        if self.key in bdata:
            data = bdata[self.key]
            if not bool(data):
                if isinstance(data, dict):
                    self.mp.insert_bson_entry(data, session)
                elif isinstance(data, list):
                    for d in data:
                        if d is not None:
                            self.mp.insert_bson_entry(d, session)


class ModelProcessor:
    def __init__(self, model):
        if not isinstance(model, DeclarativeMeta):
            raise TypeError("model should be of type DeclarativeMeta")
        self.model = model
        self.columns = [column for column in class_mapper(model).columns]
        if hasattr(model, "nested_data"):
            self.nested_data = [NestedModelProcessor(n[0], n[1], n[2]) for n in model.nested_data]
        else:
            self.nested_data = []

    def insert_bson_entry(self, bdata, session):
        obj = self.model()
        bdata = verify_dict(bdata)
        for column in self.columns:
            if "key" in column.info:
                key = column.info["key"]
            else:
                key = column.key
            if "get" in column.info:
                value = column.info["get"](bdata)
            else:
                if not column.nullable:
                    if key not in bdata:
                        raise KeyError("%s key not found" % key)
                value = bdata[key] if key in bdata else None
                if value is not None:
                    if "type" in column.info:
                        value = column.info["type"](value)
            obj.__dict__[column.key] = value
        for nest in nested_data:
            nest.insert_bson_entry(bdata, session)
        session.add(obj)
