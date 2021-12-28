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

from mo_columns.cluster import Cluster
from mo_columns.datastore import Datastore
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
            extract = cluster.find({"where": {"exists": "a"}}, result_name)
            facts = extract.get_table(result_name)
            result = facts.query({"from": result_name, "where": {"exists": "a"}, "format": "list", "limit": 1000})
            self.assertEqual(len(result.data), 20)

    def test_insert_many(self):
        db = Datastore("testing", "temp/testing")
        db.insert([{"a": randoms.base64(8), "b": randoms.base64(12)} for _ in range(5_000)])
        db.insert([{"a": randoms.base64(8), "b": randoms.base64(12)} for _ in range(10_000)])
        db.insert([{"a": randoms.base64(8), "b": randoms.base64(12)} for _ in range(20_000)])
        db.insert([{"a": randoms.base64(8), "b": randoms.base64(12)} for _ in range(40_000)])
