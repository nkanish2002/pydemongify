"Utilities"
import json
import bson


def bson_reader(fname):
    "Method for reading BSON files"
    bson_file = open(fname, "rb")
    bdata = bson_file.read()
    bson_file.close()
    return bson.decode_all(bdata)


def _decode_list(data):
    "Function to convert unicode strings in an array to utf-8"
    real_value = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        real_value.append(item)
    return real_value


def _decode_dict(data):
    "Function to convert unicode strings in an dict to utf-8"
    real_value = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)
        real_value[key] = value
    return real_value


def decode_json(string):
    "To decode a stringified json to UTF-8 encoded data dictionary."
    data = json.loads(string)
    if isinstance(data, list):
        return _decode_list(data)
    elif isinstance(data, dict):
        return _decode_dict(data)
    else:
        return None


def verify_dict(data):
    "Converts unicodes in an dict to utf-8"
    if isinstance(data, list):
        return _decode_list(data)
    elif isinstance(data, dict):
        return _decode_dict(data)
    else:
        return None
