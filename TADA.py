import os
import re
import sys
import pwd
import time
import json
import socket
import hashlib
import binascii
import subprocess
import warnings

import inspect
import importlib
import logging

from StringIO import StringIO

log = logging.getLogger(__name__)

warnings.filterwarnings('ignore') # to suppress mysql bogus warnings
                                  # in CREATE TABLE IF NOT EXISTS

LOGIN = pwd.getpwuid(os.geteuid())[0]

class Test(object):
    """TADA Test Utility

    Test scripts use this class to create and report test results to `tadad`
    server. The test script must specify:
      - test_suite : str
      - test_type : str
      - test_name : str
    And, optionally specify:
      - tada_addr : "ADDR:PORT" - for `tadad` connection
      - test_user : str - to specify `user` running the test
        (default `{LOGIN}`)
      - commit_id : str - to specify the commit_id of the target program in
        testing.

    (test_suite, test_type, test_name, test_user, commit_id) combination is used
    to identify the test in `tadad` database. This means that re-running the
    test with the same combination rewrites the result of the previous run.

    Example:
    >>> from TADA import Test
    >>> test = Test(test_suite="my suite", test_type="FVT",
    ...             test_name="simple test", commit_id="abcdefg")
    >>> test.add_assertion(1, "test True")
    >>> test.add_assertion(2, "test Skip")
    >>> test.add_assertion(3, "test False")
    >>> test.start() # this sends 'test-start' event to `tadad`
    >>> test.assert_test(1, True) # this send assertion event to `tadad`
    >>> test.assert_test(3, False) # this send assertion event to `tadad`
    >>> test.finish() # this send all skipped assertions to `tadad`, and
    >>>               # finally send `finish` event to `tadad`
    """ % { "LOGIN": LOGIN }

    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"

    def __init__(self, test_suite, test_type, test_name, test_desc = None,
                 tada_addr="localhost:9862", test_user=LOGIN,
                 commit_id="-"):
        self.test_suite = test_suite
        self.test_type = test_type
        self.test_name = test_name
        self.test_user = test_user
        self.commit_id = commit_id
        self.test_desc = test_desc if test_desc else test_name
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
        msg["test-id"] = self.test_id
        if type(msg) != str:
            msg = json.dumps(msg)
        self.sock_fd.sendto(msg.encode('utf-8'),
                            (self.tada_host, self.tada_port))

    def start(self):
        s = "{t.test_suite}:{t.test_type}:{t.test_name}:{t.test_user}:" \
            "{t.commit_id}:{ts}".format(t = self, ts = int(time.time()))
        self.test_id = binascii.hexlify(hashlib.sha256(s).digest())
        log.info("starting test `{}`".format(self.test_name))
        log.info("  test-id: {}".format(self.test_id))
        log.info("  test-suite: {}".format(self.test_suite))
        log.info("  test-name: {}".format(self.test_name))
        log.info("  test-user: {}".format(self.test_user))
        log.info("  commit-id: {}".format(self.commit_id))
        msg = {
                "msg-type": "test-start",
                "test-suite": self.test_suite,
                "test-type": self.test_type,
                "test-name": self.test_name,
                "test-user": self.test_user,
                "commit-id": self.commit_id,
                "timestamp": time.time(),
                "test-id": self.test_id,
                "test-desc": self.test_desc,
              }
        self._send(msg)

    def add_assertion(self, number, desc):
        self.assertions[number] = {
                        "msg-type": "assert-status",
                        "assert-no": number,
                        "assert-desc": desc,
                        "assert-cond": "none",
                        "test-status": Test.SKIPPED,
                    }

    def assert_test(self, assert_no, cond, cond_str):
        msg = self.assertions[assert_no]
        msg["assert-cond"] = cond_str
        msg["test-status"] = Test.PASSED if cond else Test.FAILED
        self._send(msg)
        log.info("assertion {}, {}: {}" \
                 .format(msg["assert-no"], msg["assert-desc"],
                         msg["test-status"]))

    def finish(self):
        for num, msg in self.assertions.iteritems():
            if msg["test-status"] == Test.SKIPPED:
                self._send(msg)
                log.info("assertion {}, {}: {}" \
                         .format(msg["assert-no"], msg["assert-desc"],
                                 msg["test-status"]))
        msg = {
                "msg-type": "test-finish",
                "test-id": self.test_id,
                "timestamp": time.time(),
              }
        self._send(msg)
        log.info("test {} ended".format(self.test_name))

class SQLModel(object):
    """SQL data model base class

    Models subclassing this class (e.g. TADATestModel) must define `__table__`
    attribute for table name, `__cols__` attribute for a list of
    (COL_NAME,COL_TYPE) and `__ids__` attribute for a list of columns comprising
    primary key.

    For a `Model` subclassing this class, the application should:
    - `Model.create()` to insert a new object into the table
    - `Model.find()` to find objects matching filtering conditions
    - `Model.get()` to get exactly one object, or create it if not found

    The objects obtained using above class methods will also have column names
    as their attributes, e.g.
    >>> x = TADATestModel.get(conn, test_id = "abc")
    >>> print x.test_name

    Modifying the attributes corresponding to the columns won't make any change
    to the database until commit() is called, e.g.
    >>> x = TADATestModel.get(conn, test_id = "abc")
    >>> x.test_finish = int(time.time())
    >>> x.commit()

    The object is also iterable, yielding (COL_NAME, COL_VAL) pairs.
    """

    __table__ = "TABLE_NAME" # subclass shall override this
    __cols__ = [ ("COLUMN_NAMES", "COL_TYPE") ] # list of (NAME, TYPE) defining
                                                # table columns
    __ids__ = [ "COLUMNS_IDENTIFYING_OBJECT" ] # columns comprising primary key

    def __init__(self, conn, data):
        """App should use `get`, `find` or `create` and not call this direcly"""
        self._conn = conn
        self.__colnames__, self.__coltypes__ = zip(*self.__cols__)
        # taking care of object ID first
        self._obj_id = self.__id_from_data(data)
        self._on_data_update(data)
        self._qparam = conn_qparam(conn)

    def __iter__(self):
        for k in self.__colnames__:
            yield k, getattr(self, k)

    def __hash__(self):
        return hash(tuple(self))

    @classmethod
    def _sql_create_statement(cls):
        begin = "CREATE TABLE IF NOT EXISTS {} (".format(cls.__table__)
        body = ",".join("{} {}".format(n, t) for n,t in cls.__cols__)
        if cls.__ids__:
            pk = ", PRIMARY KEY({})".format(",".join(cls.__ids__))
        else:
            pk = ""
        end = ")"
        return begin + body + pk + end

    @classmethod
    def get(cls, _conn, **kwargs):
        """Find exactly 1 object or create with given parameters if not found

        Example:
        >>> obj = TADATestModel.get(conn, test_id = "abc")
        """
        objs = cls.find(_conn, **kwargs)
        if not objs: # no object, create it with given args
            return cls.create(_conn, **kwargs)
        if len(objs) > 1: # get condition yielding non-unique objects
            raise KeyError("`get` results not unique")
        return objs[0]

    @classmethod
    def find(cls, _conn, **kwargs):
        """Find objects matching the conditions

        Example:
        >>> objs = TADATestModel.find(conn, test_id = "abc")
        """
        cur = _conn.cursor()
        qparam = conn_qparam(_conn)
        cond = " and ".join( "{}={}".format(k, qparam) for k in kwargs.keys() )
        if cond:
            sql = "SELECT * FROM {} where {}".format(cls.__table__, cond)
        else:
            sql = "SELECT * FROM {}".format(cls.__table__)
        cur.execute(sql, map(str, kwargs.values()))
        lst = cur.fetchall()
        return [ cls(_conn, data) for data in lst ]

    @classmethod
    def create(cls, _conn, *args, **kwargs):
        """Insert a new record into the table

        This supports multiple forms of parameters:
        >>> x = TADATestModel.create(conn, test_id = "abc", test_name = "bla")
        >>> x = TADATestModel.create(conn, {"test_id": "abc", "test_name":"bla"})
        >>> x = TADATestModel.create(conn, ["abc", "bla", ...])
        >>> x = TADATestModel.create(conn, "abc", "bla", ...)

        In the first two forms, the unspecified fields will be blank. In the
        latter two forms all fields must be specified in the same order as table
        colums.
        """
        # infer obj from *args and **kwargs
        if args:
            if len(args) == 1:
                obj = args[0]
            else:
                obj = args
        else:
            obj = kwargs
        qparam = conn_qparam(_conn)
        obj_type = type(obj)
        if obj_type == dict:
            cols = obj.keys()
            vals = obj.values()
            _id = { k: obj[k] for k in cls.__ids__ }
            sql = "INSERT INTO {} ({}) VALUES ({})" \
                .format(
                    cls.__table__,
                    ",".join( cols ),
                    ",".join( [qparam] * len(vals) )
                )
        elif obj_type in [ list, tuple ]:
            vals = obj
            _id = cls.__id_from_data(obj)
            sql = "INSERT INTO {} VALUES ({})" \
                .format(
                    cls.__table__,
                    ",".join( [qparam] * len(vals) )
                )
        cur = _conn.cursor()
        # use parameterized sql execution
        cur.execute(sql, map(str, vals))
        _conn.commit()
        return cls.get(_conn, **_id)

    def __cmp__(self, other):
        for k in self.__colnames__:
            a0 = getattr(self, k)
            a1 = getattr(other, k)
            c = cmp(a0, a1)
            if c:
                return c
        return 0

    @classmethod
    def __id_from_data(cls, data):
        if type(data) == dict:
            return { k: data[k] for k in cls.__ids__ }
        colnames, coltypes = zip(*cls.__cols__)
        idx = map(lambda x: colnames.index(x), cls.__ids__)
        obj_id = { k: data[i] for k, i in zip(cls.__ids__, idx) }
        return obj_id

    def _on_data_update(self, data):
        # update self attr according to data (list)
        for k, v in zip(self.__colnames__, data):
            setattr(self, k, v)

    def _query(self):
        cur = self._conn.cursor()
        cond = " and ".join( "{}={}".format(k, self._qparam) \
                                            for k in self._obj_id.keys() )
        sql = "SELECT * FROM {} where {}".format(self.__table__, cond)
        cur.execute(sql, map(str, self._obj_id.values()))
        return cur.fetchone()

    def reload(self):
        """Reload values from the database"""
        data = self._query()
        self._on_data_update(data)

    def as_tuple(self):
        """Returns tuple() of values (same order as the table columns)"""
        return tuple(v for k,v in self)

    def as_list(self):
        """Returns list() of values (same order as the table columns)"""
        return list(v for k,v in self)

    def as_dict(self):
        """Returns {COL_NAME: VALUE} dictionary"""
        return dict(self)

    def commit(self):
        """Commit attribute changes to the database"""
        vals = { k: v for k, v in self if k not in self._obj_id and v != None }
        update = ", ".join( "{}={}".format(k, self._qparam) \
                                            for k in vals.keys() )
        cond = " and ".join( "{}={}".format(k, self._qparam) \
                                            for k in self._obj_id.keys() )
        sql = "UPDATE {} SET {} WHERE {}".format(self.__table__, update, cond)
        cur = self._conn.cursor()
        params = vals.values() + self._obj_id.values()
        cur.execute(sql, map(str, params))
        self._conn.commit()

    def __str__(self):
        sio = StringIO()
        sio.write("(")
        sep = ""
        for k, v in self:
            sio.write("{}{}='{}'".format(sep, k, v))
            sep = ", "
        sio.write(")")
        return sio.getvalue()

    def __repr__(self):
        return str(type(self).__name__) + str(self)

    def delete(self):
        """Deletethe object from the database"""
        cond = " and ".join( "{}={}".format(k, self._qparam) \
                                        for k in self._obj_id.keys() )
        sql = "DELETE FROM {} WHERE {}".format(self.__table__, cond)
        cur = self._conn.cursor()
        cur.execute(sql, map(str, self._obj_id.values()))
        self._conn.commit()

    def __getitem__(self, key):
        return getattr(self, key)


class TADATestModel(SQLModel):
    __table__ = "TADATest"
    __cols__ = [
              ( "test_id"     , "VARCHAR(64)" )  ,
              ( "test_suite"  , "TEXT"        )  ,
              ( "test_type"   , "TEXT"        )  ,
              ( "test_name"   , "TEXT"        )  ,
              ( "test_user"   , "TEXT"        )  ,
              ( "commit_id"   , "TEXT"        )  ,
              ( "test_desc"   , "TEXT"        )  ,
              ( "test_start"  , "INTEGER"     )  ,
              ( "test_finish" , "INTEGER"     )  ,
        ]
    __ids__ = [ "test_id" ]

    @property
    def assertions(self):
        """all assertions belong to this test"""
        sql = "SELECT * FROM {} WHERE test_id={}" \
              .format(TADAAssertionModel.__table__, self._qparam)
        cur = self._conn.cursor()
        cur.execute(sql, [str(self.test_id)])
        return [ TADAAssertionModel(self._conn, d) for d in cur.fetchall() ]

    def getAssertion(self, assert_id):
        """Get or create an assertion"""
        return TADAAssertionModel.get(
                    self._conn,
                    test_id = self.test_id,
                    assert_id = assert_id
                )

    def delete(self):
        """Delete the Test and the Assertions from the database"""
        for a in self.assertions:
            a.delete()
        super(TADATestModel, self).delete()

    def equivalent(self, other):
        """Check the equivalent (same everythin except test_id and ts)"""
        ATTRS = [ "test_suite", "test_type", "test_name",
                  "test_user", "commit_id" ]
        for attr in ATTRS:
            c = cmp(getattr(self, attr), getattr(other, attr))
            if c:
                return False
        return True


class TADAAssertionModel(SQLModel):
    __table__ = "TADAAssertion"
    __cols__ = [
             ( "test_id"       , "VARCHAR(64)" )  ,
             ( "assert_id"     , "VARCHAR(64)" )  ,
             ( "assert_result" , "TEXT"        )  ,
             ( "assert_cond"   , "TEXT"        )  ,
             ( "assert_desc"   , "TEXT"        )  ,
        ]
    __ids__ = [ "test_id", "assert_id" ]


def conn_module(conn):
    """Get module from connection"""
    m = type(conn).__module__.split('.')[0]
    return importlib.import_module(m)

def conn_qparam(conn):
    """Determine query param string from connection"""
    mod = conn_module(conn)
    if mod.paramstyle == "qmark":
        return "?"
    return "%s"

def db_loc(host, port):
    if port:
        return host + ":" + str(port)
    return host

def sqlite_connect(db_mod, db_path, **kwargs):
    log.info("Connecting to sqlite database: {}".format(db_path))
    return db_mod.connect(db_path)

def pgsql_connect(db_mod, db_host="localhost", db_port=None, db_user=None,
                  db_password=None, db_database = None, **kwargs):
    log.info("Connecting to pgsql database: {}" \
             .format(db_loc(db_host, db_port)))
    return db_mod.connect(host=db_host, port=db_port, user=db_user,
                          password=db_password, database=db_database)

def mysql_connect(db_mod, db_host="localhost", db_port=None, db_user=None,
                  db_password=None, db_database = None, **kwargs):
    log.info("Connecting to mysql database: {}" \
             .format(db_loc(db_host, db_port)))
    _kwargs = dict(host=db_host, port=db_port, user=db_user,
                          passwd=db_password, db = db_database)
    _kwargs = { k:v for k,v in _kwargs.iteritems() if v != None }
    return db_mod.connect(**_kwargs)

DB_CONN_TBL = {
    "sqlite" : ( sqlite_connect , "sqlite3"  ),
    "mysql"  : ( mysql_connect  , "MySQLdb"  ),
    "pgsql"  : ( pgsql_connect  , "psycopg2" ),
    "postgres"  : ( pgsql_connect  , "psycopg2" ),
    "postgresql"  : ( pgsql_connect  , "psycopg2" ),
}

class TADA_DB(object):
    """TADA database utility

    This is a tool to connect to TADA database and manipulate (create/modify)
    objects.

    Connecting to sqlite:
    >>> db = TADA_DB(db_driver="sqlite", db_path="/PATH/TO/DB.FILE")

    Conencting to postgres:
    >>> db = TADA_DB(db_driver="pgsql", db_host="host", db_port=12345,
    ...              db_database="somedb", db_user="bla", db_password="bla")

    Connecting to mysql:
    >>> db = TADA_DB(db_driver="mysql", db_host="host", db_port=12345,
    ...              db_database="somedb", db_user="bla", db_password="bla")

    After connected successfully, `db.init_tables()` is called. The function
    creates tables required to hold TADA results if the tables do not exist.

    Application may call `db.drop_tables()` to drop (delete) all TADA tables.

    See `tadad` python program for an example usage of test data producer, and
    `tadaq` python program for an example usage of test data consumer.
    """
    def __init__(self, db_driver = "sqlite", **kwargs):
        ent = DB_CONN_TBL.get(db_driver)
        if not ent:
            raise RuntimeError("Unsupported driver: {}".format(db_driver))
        conn_fn, mod_name = ent
        db_mod = importlib.import_module(mod_name)
        self.db_mod = db_mod
        self.conn = conn_fn(self.db_mod, **kwargs)
        self.init_tables()

    MODELS = [
        TADATestModel,
        TADAAssertionModel,
    ]

    def init_tables(self):
        """Initialize TADA tables"""
        cur = self.conn.cursor()
        for m in self.MODELS:
            sql = m._sql_create_statement()
            cur.execute(sql)
        self.conn.commit()

    def drop_tables(self):
        """Drop all TADA tables"""
        cur = self.conn.cursor()
        for m in self.MODELS:
            sql = "DROP TABLE IF EXISTS {}".format(m.__table__)
            cur.execute(sql)
        self.conn.commit()

    def createTest(self, *args, **kwargs):
        """Create a new test in the DB"""
        return TADATestModel.create(self.conn, *args, **kwargs)

    def findTests(self, latest = False, *args, **kwargs):
        """Find tests matching the filtering conditions"""
        objs = TADATestModel.find(self.conn, *args, **kwargs)
        if latest:
            objs.sort(key = lambda x: x.test_start)
            _objs = dict()
            for o in objs:
                k = (o.test_suite, o.test_type, o.test_name,
                     o.test_user, o.commit_id)
                _objs[k] = o
            objs = _objs.values()
        return objs

    def getTest(self, *args, **kwargs):
        """Get the test macthing the criteria or create a new test if not found"""
        return TADATestModel.get(self.conn, *args, **kwargs)

    def purgeOldTests(self):
        """Purge all old tests"""
        objs = self.findTests()
        ATTRS = ["test_suite", "test_type", "test_name", "test_user",
                 "commit_id", "test_start"]
        def _cmp_(x, y):
            for attr in ATTRS:
                xa = getattr(x, attr)
                ya = getattr(y, attr)
                c = cmp(xa, ya)
                if c:
                    return c
            return 0
        objs.sort(cmp = _cmp_, reverse = True)
        itr = iter(objs)
        prev = next(itr)
        for obj in itr:
            if prev.equivalent(obj):
                obj.delete()
            else:
                prev = obj


if __name__ == "__main__":
    execfile(os.getenv('PYTHONSTARTUP', '/dev/null'))
