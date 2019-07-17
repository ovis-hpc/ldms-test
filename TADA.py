import os
import re
import sys
import time
import json
import socket
import subprocess

class Test(object):
    """TADA Test Facility"""

    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"

    def __init__(self, test_suite, test_type, test_name,
                 tada_addr="localhost:9862"):
        self.test_suite = test_suite
        self.test_type = test_type
        self.test_name = test_name
        if tada_addr is None:
            self.tada_host = "localhost"
            self.tada_port = 9862
        else:
            s = tada_addr.split(':')
            self.tada_host = s[0]
            if len(s) > 1:
                self.tada_port = int(s[1])
            else:
                self.tada_port = 9862
        self.assertions = dict()
        self.sock_fd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def _send(self, msg):
        if type(msg) != str:
            msg = json.dumps(msg)
        self.sock_fd.sendto(msg.encode('utf-8'),
                            (self.tada_host, self.tada_port))

    def start(self):
        msg = {
                "msg-type": "test-start",
                "test-suite": self.test_suite,
                "test-type": self.test_type,
                "test-name": self.test_name,
                "timestamp": time.time(),
              }
        self._send(msg)

    def add_assertion(self, number, desc):
        self.assertions[number] = {
                        "msg-type": "assert-status",
                        "test-suite": self.test_suite,
                        "test-type": self.test_type,
                        "test-name": self.test_name,
                        "assert-no": number,
                        "assert-desc": desc,
                        "assert-cond": "none",
                        "test-status": Test.SKIPPED,
                    }

    def _send_assert(self, assert_no):
        self._send(self.assertions[assert_no])

    def assert_test(self, assert_no, cond, cond_str):
        msg = self.assertions[assert_no]
        msg["assert-cond"] = cond_str
        msg["test-status"] = Test.PASSED if cond else Test.FAILED
        self._send(msg)

    def finish(self):
        for num, msg in self.assertions.iteritems():
            if msg["test-status"] == Test.SKIPPED:
                self._send(msg)
        msg = {
                "msg-type": "test-finish",
                "test-suite": self.test_suite,
                "test-type": self.test_type,
                "test-name": self.test_name,
                "timestamp": time.time(),
              }
        self._send(msg)

if __name__ == "__main__":
    execfile(os.getenv('PYTHONSTARTUP', '/dev/null'))
