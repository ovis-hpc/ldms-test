#!/usr/bin/python

import os
import sys
import sqlite3
from TADA import TADA_DB, TADATestModel

execfile(os.getenv("PYTHONSTARTUP", "/dev/null"))
conn = sqlite3.connect("tada_db.sqlite")
# d = TADATestModel(conn, [ "two" ])

db = TADA_DB(db_driver = "sqlite", db_path = "tada_db.sqlite")
# start afresh
db.drop_tables()
db.init_tables()

one = TADATestModel.create(conn, ["one", "suite", "NA", "test one", "root", "abcde", 10, 20])
two = db.createTest(test_id = "two", test_type = "NA")
three = db.getTest(test_id = "three", test_type = "NA")
#o = TADATestModel.get(conn, test_id = "one")
o = db.getTest(test_id = "one")
assert(o == one)

o.add_assertion(1, "assertion 1.1", "True==True", "passed")
o.add_assertion(2, "assertion 1.2", "Frue==True", "failed")

two.add_assertion(1, "assertion 2.1", "0==0", "passed")
two.add_assertion(2, "assertion 2.2", "1==0", "failed")

# try adding existing assertion
try:
    two.add_assertion(2, "assertion 2.2", "1==0", "failed")
except sqlite3.IntegrityError, e:
    print "OK"

print one
print two
for a in one.assertions:
    print a
for a in two.assertions:
    print a

# test commit() and reload()
o.test_type = "TYPE_ONE"
o.commit()
one.reload()
assert(one.test_type == "TYPE_ONE")

# test `find`
objs = db.findTest()
assert(set(objs) == set([one, two, three]))

objs = db.findTest(test_id = "two")
assert(objs == [ two ])

objs = db.findTest(test_type = "NA")
assert(set(objs) == set([two, three]))
