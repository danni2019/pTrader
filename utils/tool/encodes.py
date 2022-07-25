import datetime as dt
import pandas as pd
import numpy as np
import json


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.DataFrame):
            obj.reset_index(drop=False, inplace=True)
            return obj.to_json()
        if isinstance(obj, pd.Series):
            return obj.to_json()
        if isinstance(obj, (dt.datetime, dt.date)):
            return obj.isoformat()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(JsonEncoder, self).default(obj)


def encode(val_dict: dict):
    obj = {}
    for k, v in val_dict.items():
        if isinstance(v, pd.DataFrame):
            obj[k] = v.to_json()
        if isinstance(v, pd.Series):
            obj[k] = v.to_json()
        if isinstance(v, (dt.datetime, dt.date)):
            obj[k] = v.isoformat()
        if isinstance(v, np.integer):
            obj[k] = int(v)
        if isinstance(v, np.floating):
            obj[k] = float(v)
        if isinstance(v, np.ndarray):
            obj[k] = v.tolist()
    return obj


def decode(val_json: json, type_map: dict):
    val_map = json.loads(val_json)
    for k, v in val_map.items():
        if type_map[k] == dt.datetime:
            yield k, dt.datetime.fromisoformat(v)
        elif type_map[k] == pd.DataFrame:
            yield k, pd.read_json(v)
        elif type_map[k] == pd.Series:
            yield k, pd.read_json(v, orient='index')
        elif type_map[k] == np.ndarray:
            yield k, np.array(v)
        else:
            yield k, v
