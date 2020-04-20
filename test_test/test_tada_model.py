#!/usr/bin/python3

import os
import sys
import sqlite3
from TADA import TADA_DB, TADATestModel

exec(open(os.getenv("PYTHONSTARTUP", "/dev/null")).read())
conn = sqlite3.connect("tada_db.sqlite")
# d = TADATestModel(conn, [ "two" ])

db = TADA_DB(db_driver = "sqlite", db_path = "tada_db.sqlite")
# start afresh
db.drop_tables()
db.init_tables()

one = TADATestModel.create(conn, ["one", "suite", "NA", "test one", "root", "abcde", "description", 10, 20])
two = db.createTest(test_id = "two", test_type = "NA")
three = db.getTest(test_id = "three", test_type = "NA")
#o = TADATestModel.get(conn, test_id = "one")
o = db.getTest(test_id = "one")
assert(o == one)

def add_assertion(test, assert_id, desc, cond, result):
    a = test.getAssertion(assert_id)
    a.assert_desc = desc
    a.assert_result = result
    a.assert_cond = cond

add_assertion(o, 1, "assertion 1.1", "True==True", "passed")
add_assertion(o, 2, "assertion 1.2", "Frue==True", "failed")

add_assertion(two, 1, "assertion 2.1", "0==0", "passed")
add_assertion(two, 2, "assertion 2.2", "1==0", "failed")

print(one)
print(two)
for a in one.assertions:
    print(a)
for a in two.assertions:
    print(a)

# test commit() and reload()
o.test_type = "TYPE_ONE"
o.commit()
one.reload()
assert(one.test_type == "TYPE_ONE")

# test `find`
objs = db.findTests()
assert(set(objs) == set([one, two, three]))

objs = db.findTests(test_id = "two")
assert(objs == [ two ])

objs = db.findTests(test_type = "NA")
assert(set(objs) == set([two, three]))
