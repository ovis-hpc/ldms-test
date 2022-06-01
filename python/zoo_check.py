#!/usr/bin/python3
# Check zookeeper 10 times, 1 second interval
import socket
import time
import sys

N = 10
S = 1

def zoo_check():
    try:
        s = socket.socket()
        s.connect(('localhost', 2181))
        s.send(b'stat')
        rep = s.recv(4096)
        print("--- zookeeper reply ---")
        print(rep.decode())
        print("-----------------------")
        assert(rep.startswith(b'Zoo'))
    except:
        return -1
    return 0

for i in range(0, N):
    rc = zoo_check()
    if rc == 0:
        sys.exit(0)
    time.sleep(S)
sys.exit(-1)
