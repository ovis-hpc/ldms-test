#!/usr/bin/python3

import os
import sys

import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

ap = argparse.ArgumentParser(
            description = 'Serdes Avro Kafka consumer',
        )
ap.add_argument('-b', '--bootstrap-server', type=str, required=True,
                help='Kafka Bootstrap Server (e.g. kafka-1)')
ap.add_argument('-s', '--schema-registry-url', type=str, required=True,
                help='The Schema Registry URL (e.g. http://schema-registry-1:8081)')
ap.add_argument('-t', '--topic', type=str, required=True,
                help='The topic (e.g. meminfo)')
ap.add_argument('-c', '--count', type=int,
                help='The number of messages to receive before' \
                     ' process termination (default: -1, unlimited)',
                default = -1)

args = ap.parse_args()

sr_conf = { 'url': args.schema_registry_url }
sr_client = SchemaRegistryClient(sr_conf)
ad = AvroDeserializer(sr_client)

c = Consumer({
        'bootstrap.servers': args.bootstrap_server,
        'group.id': 'mygroup',
    })

c.subscribe([args.topic])

count = args.count

while count:
    msg = c.poll()
    obj = ad(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
    print(obj)
    if count > 0:
        count -= 1
