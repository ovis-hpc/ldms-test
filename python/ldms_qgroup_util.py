#!/usr/bin/python3
import re
import time
import logging

from collections import deque

class QGROUP(object): # a place to statically hold config params
    CFG_QUOTA = 256
    CFG_ASK_MARK = 128
    CFG_ASK_AMOUNT = 64
    CFG_ASK_USEC = 100000 # 0.1 SEC
    CFG_RESET_USEC = 1000000 # 1 SEC

RE = re.compile("""
        (?P<ts>[^ ]+\ [^ ]+) # timestamp
        \ (?P<logger>\w+)
        \ (?P<level>\w+)
        \ (?:
            (?:
                published:
                \ \[(?P<pub_name>\w+)\]:
                \ (?P<pub_data>.*)
            )
            |(?:
                pub_failed:
                \ (?P<pub_error>.*)
            )
            |(?:
                recv:
                \ (?P<recv_src>\S+)
                \ \[(?P<recv_name>\w+)\]:
                \ (?P<recv_data>.*)
            )
            |(?P<other>(?!(?:published|pub_failed|recv)).*)
        )
        """, re.X)

def parse_log(l):
    m = RE.match(l)
    return m.groupdict()
