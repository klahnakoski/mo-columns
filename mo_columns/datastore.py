# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import os
import sys
from math import floor, log

from mo_logs import Log

from jx_sqlite.sqlite import Sqlite, quote_column
from mo_columns.cluster import Cluster, get_key_columns
from mo_dots import Null
from mo_future import sort_using_key
from mo_threads import Lock, Till, Thread
from mo_times import Timer
from vendor.mo_files import File
from vendor.mo_json.types import T_JSON, union_type

MAX_INT = sys.maxsize
MAX_CLUSTER_SIZE = 1_000_000
CLUSTER_NAME_LENGTH = 5
START_CLUSTER_NAME = "0" * CLUSTER_NAME_LENGTH
MERGE_RATIO = 3
IGNORE_PREFIX = "temp_"


sql_code_to_sql_type = {}


class Datastore(object):
    def __init__(self, name, dir):
        self.name = name
        self.dir = File(dir)
        self.schema = T_JSON
        self.active_cluster = None
        self.cluster_locker = Lock(self.dir.abspath + " clusters")
        self.clusters = [
            Cluster(d)
            for d in self.dir.children
            if d.is_directory() and not d.name.startswith(IGNORE_PREFIX)
        ]
        if not self.clusters:
            self.max_cluster = START_CLUSTER_NAME
            self.active_cluster = Cluster(self.dir / self._next_cluster())
        else:
            self.max_cluster = max(int(c.dir.name) for c in self.clusters)
        self.merge_thread = Thread.run(
            "merge daemon for " + self.dir.abspath, self._merge_worker
        )
        min_size = MAX_INT
        for c in self.clusters:
            bs = c.bytes()
            if bs < min_size:
                self.active_cluster = c
                min_size = bs

        self.active_cluster.open()
        # TODO: SWITCH TO MERGE ALL DATABASES INTO ONE?  DOES IT MATTER?  DOES ZIP WORK?

    def _next_cluster(self):
        self.max_cluster = (START_CLUSTER_NAME + str(int(self.max_cluster) + 1))[-CLUSTER_NAME_LENGTH:]
        return self.max_cluster

    def insert(self, documents) -> Cluster:
        """
        LOAD documents INTO SQLITE DATABASES REPRESENTING COLUMNS
        :param documents:
        :return: cluster
        """
        if self.active_cluster.bytes() > MAX_CLUSTER_SIZE:
            # MAKE NEW CLUSTER
            self.active_cluster.close()
            self.clusters.append(self.active_cluster)
            self.active_cluster = Cluster(self.dir / self._next_cluster())

        return self.active_cluster.add(documents)

    def _merge(self, *clusters) -> Cluster:
        """
        MERGE A NUMBER OF CLUSTERS INTO ONE
        :param clusters: list of clusters
        :return: merged cluster
        """
        new_schema = union_type(*(c.schema for c in clusters))
        new_cluster = Cluster(File(IGNORE_PREFIX + self._next_cluster()))

        for path in new_schema:
            # ATTACH CLUSTERS WITH COLUMN
            existing_column_files = [
                c.dir / path for c in self.clusters if path in c.schema
            ]
            db = new_cluster.columns[path] = Sqlite(new_cluster.dir / path)
            with db.transaction() as t:
                t.execute(";\n".join(
                    f"ATTACH {f} AS db{i}" for i, f in enumerate(existing_column_files)
                ))
                new_cluster._add_column(path, db)
                key_columns = get_key_columns(path)
                t.execute(
                    f"INSERT INTO {quote_column(path)}({key_columns}, value)"
                    + " UNION ALL ".join(
                        f"SELECT {key_columns}, value FROM db{i}.{quote_column(path)}"
                        for i, _ in enumerate(clusters)
                    )
                )
            db.close()
        return new_cluster

    def _merge_worker(self, please_stop):
        while not please_stop:
            (please_stop | Till(seconds=30)).wait()
            if not self.clusters:
                continue

            while not please_stop:
                clusters = list(sort_using_key(
                    [(c.bytes(), c) for c in self.clusters], lambda s: s[0]
                ))
                scale = floor(log(clusters[0][0]) / log(3))
                min_size = pow(3, scale)
                max_size = min_size * MERGE_RATIO
                if any(b >= max_size for b, _ in clusters[0:MERGE_RATIO]):
                    break

                old_clusters = self.clusters[0:MERGE_RATIO]
                merged = self._merge(*old_clusters)
                with self.cluster_locker:
                    # DROP OLD CLUSTERS
                    new_dir = merged.dir.parent / merged.dir.name[len(IGNORE_PREFIX) :]
                    self.clusters = [Cluster(new_dir)] + self.clusters[MERGE_RATIO:]
                    os.rename(merged.dir.abspath, new_dir.abspath)
                    # START DANGER - RESTART WILL SCAN THIS dir WITH EXTRA CLUSTERS

                with Timer("delete old clusters"):
                    to_delete = []
                    for c in old_clusters:
                        c.db.close()
                        temp = c.dir.parent / (IGNORE_PREFIX + c.dir.name)
                        to_delete.append(temp)
                        os.rename(c.dir.abspath, temp)
                    for d in to_delete:
                        d.delete()
                    # END DANGER

    def delete(self):
        self.dir.delete()

    def query(self, query) -> Cluster:
        """
        SIMPLE AGGREGATE OVER SOME SUBSET, GROUPED BY SOME OTHER COLUMNS
        :param query:
        :return: cluster
        """


        return Null


    def get_document(self, _id):
        """
        RETURN DOCUMENT WITH GIVEN UUID
        """

        # BROADCAST QUERY
        result=[]
        for c in self.clusters:
            result.append(c.get_document)
        result.append(self.active_cluster.query(query))
        return result

        # AGGREGATE RESULT
        if query['from']!=self.name:
            Log.error("please fix me")




    def matmul(self, a, b) -> Cluster:
        """
        ASSUMING a AND b REFER TO MULTIDIMENSIONAL ARRAYS
        :return: MATRIX MULTIPLY IN A CLUSTER
        """
