#!/usr/bin/python3

import io
import os
import sys
import json
import avro.schema
import avro.io

from confluent_kafka.schema_registry import SchemaRegistryClient

from ovis_ldms import ldms


# connection
x = ldms.Xprt("sock")
x.connect("localhost", "411")
x.stream_subscribe("avro", False)

sch_def = {
        "namespace": "us.ogc",
        "type": "record",
        "name": "Person",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "uid", "type": "int"},
            {"name": "gid", "type": "int"},
        ]
    }
sch_json = json.dumps(sch_def)

avro_sch = avro.schema.parse(sch_json)

sr_conf = { "url": "http://schema-registry-1:8081", }

sr_cli = SchemaRegistryClient(sr_conf)

# stream client
sc = ldms.StreamClient("avro", False, sr_client = sr_cli)

# Avro stuff
def make_avro(obj: object, sch):
    buff = io.BytesIO()
    dw = avro.io.DatumWriter(sch)
    be = avro.io.BinaryEncoder(buff)
    dw.write(rec, be)
    data = buff.getvalue()
    return data

def make_obj(payload: bytes, sch):
    buff = io.BytesIO(payload)
    dr = avro.io.DatumReader(sch)
    de = avro.io.BinaryDecoder(buff)
    obj = dr.read(de)
    return obj
