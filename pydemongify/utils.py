from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import create_engine
import json
import bson


def bson_reader(fname):
    f = open(fname, "rb")
    bdata = f.read()
    f.close()
    return bson.decode_all(bdata)


def _decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        rv.append(item)
    return rv


def _decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)
        rv[key] = value
    return rv


def decode_json(string):
    """
        To decode a stringified json to UTF-8 encoded data dictionary.
    """
    data = json.loads(string)
    if isinstance(data, list):
        return _decode_list(data)
    elif isinstance(data, dict):
        return _decode_dict(data)
    else:
        return None


def verify_dict(data):
    if isinstance(data, list):
        return _decode_list(data)
    elif isinstance(data, dict):
        return _decode_dict(data)
    else:
        return None
