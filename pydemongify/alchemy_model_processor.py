"""Contains functions for processing sqlalchemy models"""
from datetime import datetime
import os
import bson

from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import class_mapper

from .utils import verify_dict

DATA_TYPES = {
    "String": str,
    "Integer": int,
    "Float": float,
    "DateTime": datetime,
    "BigInteger": long,
    "Boolean": bool
}


class NestedModelProcessor:
    """For processing nested data models"""
    def __init__(self, key, model, foreign_key_list, session):
        self.key = key
        self.model = model
        self.fklist = foreign_key_list
        self.session = session
        self.model_processor = ModelProcessor(model, session)

    def insert(self, bdata):
        "Inserting documents into "
        if self.key in bdata:
            data = bdata[self.key]
            if not bool(data):
                if isinstance(data, dict):
                    self.model_processor.insert(data)
                elif isinstance(data, list):
                    for d in data:
                        if d is not None:
                            self.model_processor.insert(d)


class ModelProcessor:
    "For processing SQL Alchemy models"
    def __init__(self, model, session):
        if not isinstance(model, DeclarativeMeta):
            raise TypeError("model should be of type DeclarativeMeta")
        self.model = model
        self.session = session
        self.columns = [column for column in class_mapper(model).columns]
        if hasattr(model, "nested_data"):
            self.nested_data = [
                NestedModelProcessor(n[0], n[1], n[2], self.session) for n in model.nested_data
            ]
        else:
            self.nested_data = []

    def insert(self, bdata, commit=True):
        "Creates an entry into SQL from the given bdata"
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
        for nest in self.nested_data:
            nest.insert(bdata)
        self.session.add(obj)
        if commit:
            try:
                self.session.commit()
            except Exception as error:
                print(str(error))
                self.session.rollback()

    def insert_from_file(self, fname, batch=0):
        "Inserts records from backup bson files"
        if os.path.isfile(fname):
            fdata = open(fname, 'rb')
            count = 0
            for entry in fdata:
                bdata = bson.decode_all(entry)
                if batch == 0:
                    self.insert(bdata, commit=True)
                else:
                    self.insert(bdata, commit=(count % batch == 0))

