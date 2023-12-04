#!/usr/bin/python3

import json

from ovis_ldms import ldms

x = ldms.Xprt("sock")
x.connect("localhost")

s00 = {
        "schema": "s0",
        "count": 0,
        "val": 0.0
    }

s01 = {
        "schema": "s0",
        "count": 1,
        "val": 1.1
    }

s10 = {
        "schema": "s1",
        "count": 1,
        "tx": 10,
        "rx": 20,
    }

s11 = {
        "schema": "s1",
        "count": 2,
        "tx": 11,
        "rx": 21,
    }
