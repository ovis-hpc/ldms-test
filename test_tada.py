#!/usr/bin/python
# This is a script to feed several tests to tadad

import os
import sys
from TADA import Test

if sys.flags.interactive:
    execfile(os.getenv("PYTHONSTARTUP", "/dev/null"))

t = Test(test_suite="LDMSD",
         test_type="Test", test_name="TestTADA",
         commit_id = "abcdefg")
t.add_assertion(1, "Test true")
t.add_assertion(2, "Test skip")
t.add_assertion(3, "Test false")

t.start()
t.assert_test(1, True, "True")
t.assert_test(3, False, "False")
t.finish()

# same test, different user
t2 = Test(test_suite="LDMSD",
         test_type="Test", test_name="TestTADA",
         test_user = "root",
         commit_id = "abcdefg")
t2.add_assertion(1, "Test true")
t2.add_assertion(2, "Test skip")
t2.add_assertion(3, "Test false")

t2.start()
t2.assert_test(1, True, "True")
t2.assert_test(3, False, "False")
t2.finish()

# same test, different user and commit ID
t3 = Test(test_suite="LDMSD",
         test_type="Test", test_name="TestTADA",
         test_user = "root",
         commit_id = "blabla")
t3.add_assertion(1, "Test true")
t3.add_assertion(2, "Test skip")
t3.add_assertion(3, "Test false")

t3.start()
t3.assert_test(1, True, "True")
t3.assert_test(3, False, "False")
t3.finish()

# rerun of test#1 with different result
t = Test(test_suite="LDMSD",
         test_type="Test", test_name="TestTADA",
         commit_id = "abcdefg")
t.add_assertion(1, "Test true")
t.add_assertion(2, "Test skip")
t.add_assertion(3, "Test false")

t.start()
t.assert_test(1, True, "True")
t.assert_test(2, True, "True")
t.assert_test(3, True, "False")
t.finish()
