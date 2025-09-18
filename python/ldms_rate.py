#!/usr/bin/python3
import os
import sys
import pdb
import time
from ovis_ldms import ldms

ldms.init(16*1024*1024)

PORT = 411

x = ldms.Xprt("sock", rail_rate_limit = 64)

cli0 = None # msg 'test.*' client
cli1 = None # msg 'rate.*' client
err0 = list()
err1 = list()

def sub_routine():
    # subscriber routine
    global x, cli0, cli1
    cli0 = ldms.MsgClient('test.*', True)
    cli1 = ldms.MsgClient('rate.*', True)
    x.msg_subscribe('test.*', True)
    x.msg_subscribe('rate.*', True, rx_rate = 32)

def pub(msg, n, err_list):
    for I in range(0, n):
        try:
            ldms.msg_publish(msg, 16*f"{I:x}")
        except Exception as e:
            err_list.append( (I, e) )

def listen():
    global x
    x.listen()

def connect():
    global x
    x.connect('localhost')

def pub_routine():
    global err0, err1
    pub('test', 16, err0)
    time.sleep(2)
    ldms.msg_publish('test', 28*'x')
    time.sleep(2)
    pub('rate', 16, err1)
    time.sleep(2)
    ldms.msg_publish('rate', 28*'x')
