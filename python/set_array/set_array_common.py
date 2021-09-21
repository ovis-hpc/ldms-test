#!/usr/bin/python3

import os
import sys
import time
import logging
import json

from ovis_ldms import ldms

PORT=10000
ARRAY_CARD = 3

class Global(object): pass
G = Global()

def set_dict(s):
    _set = dict( name = s.name,
                 meta_gn = s.meta_gn,
                 data_gn = s.data_gn,
                 ts = s.transaction_timestamp,
                 metrics = s.as_dict() )
    return _set

def dict_list(d):
    """Recursively convert dict to Key-Value list"""
    return tuple( (k, dict_list(v)) if type(v) is dict else (k, v) for k, v in d.items() )

def jprint(obj):
    print(json.dumps(obj, indent=2))

def print_set(s):
    jprint(set_dict(s))
