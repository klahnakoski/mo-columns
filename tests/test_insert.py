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


class TestInsert(FuzzyTestCase):

    def test_insert_into_cluster(self):
        cluster = Cluster("testing", "temp/testing/0")
        cluster.insert({"a": randoms.base64(6), "b": randoms.base64(12)} for _ in range(2_000))
        result = cluster.query({"where": "exists":"a"})


    def test_insert_many(self):
        db = Datastore("testing", "temp/testing")
        db.insert([{"a": randoms.base64(8), "b": randoms.base64(12)} for _ in range(5_000)])
        db.insert([{"a": randoms.base64(8), "b": randoms.base64(12)} for _ in range(10_000)])
        db.insert([{"a": randoms.base64(8), "b": randoms.base64(12)} for _ in range(20_000)])
        db.insert([{"a": randoms.base64(8), "b": randoms.base64(12)} for _ in range(40_000)])
