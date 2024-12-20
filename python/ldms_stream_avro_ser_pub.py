#!/usr/bin/python3

import io
import os
import sys
import json

import avro.schema
import avro.io

from confluent_kafka.schema_registry import SchemaRegistryClient

from ovis_ldms import ldms


# LDMS connection
x = ldms.Xprt("sock")
x.connect("localhost", 411)

# Schema Registry
sr_conf = { "url": "http://schema-registry-1:8081", }
sr_cli = SchemaRegistryClient(sr_conf)


sch_def = {
        "namespace": "us.ogc",
        "type": "record",
        "name": "Job",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "cmd", "type": "string"},
            {"name": "uid", "type": "int"},
            {"name": "gid", "type": "int"},
            {"name": "pid", "type": "int"},
        ]
    }
data = {"name": "job1", "cmd": "/bin/bash", "uid": 1001, "gid":2001, "pid": 111}
x.stream_publish("avro", data, stream_type=ldms.LDMS_STREAM_AVRO_SER,
                 sr_client = sr_cli, schema_def = sch_def)
