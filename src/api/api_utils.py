import json
from datetime import datetime


def defaultconverter(o):
    if isinstance(o, datetime):
        return o.__str__()

def json_ser_datetime(data):
    return json.dumps(data, default=defaultconverter)