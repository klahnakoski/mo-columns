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
from mo_columns.cluster import Cluster
from mo_files import File
from mo_times import Timer

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
        num = 10_000
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
        sqlite.DEBUG = False
        num = 1_000  # million = took 419.861 seconds
        result_name = "temp_result"
        File(f"{result_name}.sqlite").delete()

        cluster = Cluster(File(CLUSTER_DIR) / "0")
        with Timer("insert records"):
            cluster.insert_using_json((
                {
                    "a": randoms.base64(6),
                    "b": randoms.float(),
                    "]": True,
                    "c": [randoms.float(), {"x": randoms.float()}],
                }
                for _ in range(num)
            ))

        with Timer("time to count"):
            self.assertEqual(cluster.count(), num)

        with Timer("extract records"):
            extract = cluster.to_rows(result_name)

    def test_multiply(self):
        # INSERT GRIDS
        sqlite.DEBUG = False
        num = 100
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
        # MULTIPLY
        pass
