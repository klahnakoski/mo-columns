# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_math import randoms
from mo_testing.fuzzytestcase import FuzzyTestCase

from jx_sqlite import sqlite
from jx_sqlite.sqlite import sql_create, Sqlite, sql_insert, ConcatSQL, SQL_CREATE, quote_column, sql_alias, sql_list, \
    SQL_SELECT, SQL_AS, SQL, SQL_FROM, SQL_INSERT, quote_value, SQL_COMMA, SQL_LEFT_JOIN, SQL_ON, SQL_GROUPBY, \
    SQL_CROSS_JOIN
from mo_columns.cluster import Cluster
from mo_files import File
from mo_logs import Log
from mo_threads import Till, Thread
from mo_times import Timer, Date

CLUSTER_DIR = "temp/testing"


class TestInsert(FuzzyTestCase):
    def setUp(self):
        File(CLUSTER_DIR).delete()

    def test_insert_into_cluster(self):
        result_name = "temp_result"
        File(f"{result_name}.sqlite").delete()

        cluster = Cluster(File(CLUSTER_DIR) / "0")
        data = [{"a": randoms.base64(6), "b": randoms.base64(12)} for _ in range(20)]
        cluster.insert(data)

        self.assertEqual(cluster.count(), 20)
        with Timer("get all records"):
            extract = cluster.to_rows(result_name)
            facts = extract.get_table(result_name)
            result = facts.query({
                "from": result_name,
                "where": {"exists": "a"},
                "format": "list",
                "limit": 1000,
            })
            self.assertEqual(len(result.data), 20)

    def test_insert_many(self):
        sqlite.DEBUG = False
        num = 10_000  # insert 100000 records (took 31.121 seconds)
        result_name = "temp_result"
        File(f"{result_name}.sqlite").delete()

        cluster = Cluster(File(CLUSTER_DIR) / "0")
        with Timer("insert {{num}} records", {"num": num}):
            cluster.insert((
                {
                    "a": randoms.base64(6),
                    "b": randoms.float(),
                    "c": [randoms.float(), {"x": randoms.float()}],
                }
                for _ in range(num)
            ))

        with Timer("time to count"):
            self.assertEqual(cluster.count(), num)

        with Timer("extract records"):
            extract = cluster.to_rows(result_name)

    def test_insert_million(self):
        # sqlite.DEBUG = False
        num = 1_000  # million => load data (took 69.137 seconds), insert records (took 123.053 seconds)
        result_name = "temp_result"
        File(f"{result_name}.sqlite").delete()

        cluster = Cluster(File(CLUSTER_DIR) / "0")
        with Timer("insert records"):
            cluster.insert_using_json((
                {
                    "a": randoms.base64(6),
                    "b": randoms.float(),
                    "]": True,
                    "k": randoms.sample(("apple", "orange", "pear", "grape", "melon", "cheese", "steak"), 1)[0],
                    "c": [randoms.float(), {"x": randoms.float()}],
                }
                for _ in range(num)
            ))

        with Timer("time to count"):
            self.assertEqual(cluster.count(), num)

        with Timer("extract records"):
            extract = cluster.to_rows(result_name)

    def test_grid(self):
        # INSERT GRIDS
        # sqlite.DEBUG = False
        num = 10
        result_name = "temp_result"
        File(f"{result_name}.sqlite").delete()

        cluster = Cluster(File(CLUSTER_DIR) / "0")
        with Timer("insert records"):
            cluster.insert_using_json((
                {
                    "d": [[randoms.float() for _ in range(100)] for _ in range(100)]
                }
                for _ in range(num)
            ))

        with Timer("extract records"):
            extract = cluster.to_rows(result_name)

    def test_multiply(self):
        """
        TEST MULTIPLY GRID WHERE EACH ELEMENT IS ROW (postal_code, attribute, value)
        """
        sqlite.DEBUG = False
        num_post = 50_000
        attributes = [randoms.base64(10) for _ in range(1_000)]

        sqlite.DEBUG = True
        db = Sqlite(filename=File(CLUSTER_DIR)/"pre_data.sqlite")
        db.query("PRAGMA synchronous = OFF")
        with db.transaction() as t:
            t.execute(sql_create("attr", {"name": "text"}))
            t.execute(sql_insert("attr", [{"name": a} for a in attributes]))
            t.execute(sql_create("post", {"name": "text"}, ["name"]))
            t.execute(ConcatSQL(
                SQL_INSERT,
                quote_column("post"),
                SQL("(name)"),
                SQL(f"""
                    WITH RECURSIVE counter(value) AS (
                    SELECT 1 
                    UNION ALL
                    SELECT value+1 FROM counter WHERE value<{num_post}
                    )
                """),
                SQL_SELECT,
                SQL("SUBSTR('abcdefghijklmnopqrstuvwxyz0123456789', random()%36 +1, 1) || SUBSTR('abcdefghijklmnopqrstuvwxyz0123456789', random()%36 +1, 1) || SUBSTR('abcdefghijklmnopqrstuvwxyz0123456789', random()%36 +1, 1) || SUBSTR('abcdefghijklmnopqrstuvwxyz0123456789', random()%36 +1, 1) || SUBSTR('abcdefghijklmnopqrstuvwxyz0123456789', random()%36 +1, 1) || SUBSTR('abcdefghijklmnopqrstuvwxyz0123456789', random()%36 +1, 1) || SUBSTR('abcdefghijklmnopqrstuvwxyz0123456789', random()%36 +1, 1)"),
                SQL_FROM,
                quote_column("counter")
            ))

        # MAIN MATRIX
        with db.transaction() as t:
            # PRIMARY KEY LENGTHENS TIME CREATE THE grid
            # 20K => (took 18.427 seconds)
            # 50K => (took 47.779 seconds)
            t.execute(sql_create("grid", {"post": "text", "attr": "text", "value": "integer"}, ["post", "attr"]))
            t.execute(ConcatSQL(
                SQL_INSERT,
                quote_column("grid"),
                SQL_SELECT,
                sql_list([
                    sql_alias(quote_column("p", "name"), "post"),
                    sql_alias(quote_column("a", "name"), "attr"),
                    SQL("abs(random() % 50) as value")
                ]),
                SQL_FROM,
                # ENSURE THESE ARE IN SAME ORDER AS PRIMARY KEY, OR IT WILL BE SLOW
                SQL("(SELECT name FROM post ORDER BY name) AS p"),
                SQL_CROSS_JOIN,  # CROSS JOIN ENFORCES LOOP ORDER
                sql_alias(quote_column("attr"), "a"),
            ))
            # INDEX DOES NOT GO FASTER
            # t.execute("CREATE INDEX aaa ON grid (post, attr, value)")

        with db.transaction() as t:
            t.execute(sql_create("file", {"file": "text", "post": "text", "value": "integer"}))
            t.execute(ConcatSQL(
                SQL_INSERT,
                quote_column("file"),
                SQL("(file, post, value)"),
                SQL_SELECT,
                sql_list([quote_value("file1"), quote_column("p", "name"), SQL("random()%50")]),
                SQL_FROM,
                sql_alias(quote_column("post"), "p"),
            ))

        def counter(please_stop):
            start = Date.now()
            while not please_stop:
                (please_stop | Till(seconds=3)).wait()
                Log.note("waiting for {{seconds}}", seconds=(Date.now()-start).seconds)

        # 10K => multiply (took 17.162 seconds)
        # 20K => multiply (took 36.514 seconds)
        # 30K => multiply (took 56.337 seconds)
        # 40K => multiply (took 75.337 seconds)
        # 70K => multiply (took 137.564 seconds)

        # With primary key on grid
        # 10k => multiply (took 11.817 seconds)
        # 20k => multiply (took 24.241 seconds)
        # 50k => multiply (took 64.271 seconds)
        with Timer("multiply"):
            timer = Thread.run("monitor", counter)
            with db.transaction() as t:
                t.execute(ConcatSQL(
                    SQL_CREATE,
                    quote_column("result"),
                    SQL_AS,
                    SQL_SELECT,
                    SQL("f.file"),
                    SQL_COMMA,
                    SQL("f.post"),
                    SQL_COMMA,
                    SQL("sum(g.value*f.value)/count(g.value) AS value"),
                    SQL_FROM,
                    sql_alias(quote_column("grid"), "g"),
                    SQL_LEFT_JOIN,
                    sql_alias(quote_column("file"), "f"),
                    SQL_ON,
                    SQL("f.post=g.post"),
                    SQL_GROUPBY,
                    SQL("f.file"),
                    SQL_COMMA,
                    SQL("f.post"),
                ))
            timer.stop()

    def test_multiply_ints(self):
        """
        TEST MULTIPLY GRID WHERE EACH ELEMENT IS ROW (x, y, value)
        """
        sqlite.DEBUG = False
        num_post = 50_000
        attributes = [i for i in range(1_000)]

        sqlite.DEBUG = True
        db = Sqlite(filename=File(CLUSTER_DIR)/"pre_data.sqlite")
        # db = Sqlite()
        db.query("PRAGMA synchronous = OFF")
        with db.transaction() as t:
            t.execute(sql_create("attr", {"name": "integer"}))
            t.execute(sql_insert("attr", [{"name": a} for a in attributes]))
            t.execute(sql_create("post", {"name": "integer"}, ["name"]))
            t.execute(ConcatSQL(
                SQL_INSERT,
                quote_column("post"),
                SQL("(name)"),
                SQL(f"""
                    WITH RECURSIVE counter(value) AS (
                    SELECT 1 
                    UNION ALL
                    SELECT value+1 FROM counter WHERE value<{num_post}
                    )
                """),
                SQL_SELECT,
                SQL("value"),
                SQL_FROM,
                quote_column("counter")
            ))

        # MAIN MATRIX
        with db.transaction() as t:
            # PRIMARY KEY LENGTHENS TIME CREATE THE grid
            # 50K => (took 30.728 seconds)
            t.execute(sql_create("grid", {"post": "integer", "attr": "integer", "value": "integer"}, ["post", "attr"]))
            t.execute(ConcatSQL(
                SQL_INSERT,
                quote_column("grid"),
                SQL_SELECT,
                sql_list([
                    sql_alias(quote_column("p", "name"), "post"),
                    sql_alias(quote_column("a", "name"), "attr"),
                    SQL("abs(random() % 50) as value")
                ]),
                SQL_FROM,
                # ENSURE THESE ARE IN SAME ORDER AS PRIMARY KEY, OR IT WILL BE SLOW
                SQL("(SELECT name FROM post ORDER BY name) AS p"),
                SQL_CROSS_JOIN,  # CROSS JOIN ENFORCES LOOP ORDER
                sql_alias(quote_column("attr"), "a"),
            ))
            # INDEX DOES NOT GO FASTER
            # t.execute("CREATE INDEX aaa ON grid (post, attr, value)")

        with db.transaction() as t:
            t.execute(sql_create("file", {"file": "text", "post": "text", "value": "integer"}))
            t.execute(ConcatSQL(
                SQL_INSERT,
                quote_column("file"),
                SQL("(file, post, value)"),
                SQL_SELECT,
                sql_list([quote_value("file1"), quote_column("p", "name"), SQL("random()%50")]),
                SQL_FROM,
                sql_alias(quote_column("post"), "p"),
            ))

        def counter(please_stop):
            start = Date.now()
            while not please_stop:
                (please_stop | Till(seconds=3)).wait()
                Log.note("waiting for {{seconds}}", seconds=(Date.now()-start).seconds)

        # With primary key on grid
        # 10k => multiply (took 7.926 seconds)
        # 50k => multiply (took 43.481 seconds)
        with Timer("multiply"):
            timer = Thread.run("monitor", counter)
            with db.transaction() as t:
                t.execute(ConcatSQL(
                    SQL_CREATE,
                    quote_column("result"),
                    SQL_AS,
                    SQL_SELECT,
                    SQL("f.file"),
                    SQL_COMMA,
                    SQL("f.post"),
                    SQL_COMMA,
                    SQL("sum(g.value*f.value)/count(g.value) AS value"),
                    SQL_FROM,
                    sql_alias(quote_column("file"), "f"),
                    SQL_CROSS_JOIN,
                    sql_alias(quote_column("grid"), "g"),
                    SQL_ON,
                    SQL("f.post=g.post"),
                    SQL_GROUPBY,
                    SQL("f.file"),
                    SQL_COMMA,
                    SQL("f.post"),
                ))
            timer.stop()



