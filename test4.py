#!/usr/bin/env python

import os
import sys
import socket

from LDMS_Test import Test

t = Test("one", "two", "three")

t.add_assertion(1, "one")
t.add_assertion(2, "two")
t.add_assertion(3, "three")

t.start()

t.assert_test(1, True, "True")
t.assert_test(3, False, "False")

t.finish()
