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

from jx_sqlite.sqlite import Sqlite, quote_column, sql_list
from mo_columns.shard import Shard
from mo_dots import Null
from mo_future import sort_using_key
from mo_threads import Lock, Till, Thread
from mo_times import Timer
from mo_files import File
from mo_json.types import T_JSON, union_type

MAX_INT = sys.maxsize
MIN_CLUSTER_SIZE = 7 * 1024 * 1024
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
        self.active_shard = None
        self.shard_locker = Lock(self.dir.abs_path + " shards")
        self.shards = [
            Shard(d)
            for d in self.dir.children
            if d.is_directory() and not d.name.startswith(IGNORE_PREFIX)
        ]
        if not self.shards:
            self.max_shard = START_CLUSTER_NAME
            self.active_shard = Shard(self.dir / self._next_shard())
        else:
            self.max_shard = max(int(c.dir.name) for c in self.shards)
        self.merge_thread = Thread.run(
            "merge daemon for " + self.dir.abs_path, self._merge_worker
        )
        min_size = MAX_INT
        for c in self.shards:
            bs = c.bytes()
            if bs < min_size:
                self.active_shard = c
                min_size = bs

        self.active_shard.open()
        # TODO: SWITCH TO MERGE ALL DATABASES INTO ONE?  DOES IT MATTER?  DOES ZIP WORK?

    def _next_shard(self):
        self.max_shard = (START_CLUSTER_NAME + str(int(self.max_shard) + 1))[-CLUSTER_NAME_LENGTH:]
        return self.max_shard

    def insert(self, documents) -> Shard:
        """
        LOAD documents INTO SQLITE DATABASES REPRESENTING COLUMNS
        :param documents:
        :return: shard
        """
        if self.active_shard.bytes() > MIN_CLUSTER_SIZE:
            # MAKE NEW CLUSTER
            self.active_shard.close()
            self.shards.append(self.active_shard)
            self.active_shard = Shard(self.dir / self._next_shard())

        return self.active_shard.add(documents)

    def _merge(self, *shards) -> Shard:
        """
        MERGE A NUMBER OF CLUSTERS INTO ONE
        :param shards: list of shards
        :return: merged shard
        """
        new_schema = union_type(*(c.schema for c in shards))
        new_shard = Shard(File(IGNORE_PREFIX + self._next_shard()))

        for path in new_schema:
            # ATTACH CLUSTERS WITH COLUMN
            existing_column_files = [
                c.dir / path for c in self.shards if path in c.schema
            ]
            db = new_shard.columns[path] = Sqlite(new_shard.dir / path)
            with db.transaction() as t:
                t.execute(";\n".join(
                    f"ATTACH {f} AS db{i}" for i, f in enumerate(existing_column_files)
                ))
                new_shard._add_column(path, db)
                key_columns = sql_list(list(map(quote_column, get_key_columns(path))))
                t.execute(
                    f"INSERT INTO {quote_column(path)}({key_columns}, value)"
                    + " UNION ALL ".join(
                        f"SELECT {key_columns}, value FROM db{i}.{quote_column(path)}"
                        for i, _ in enumerate(shards)
                    )
                )
            db.close()
        return new_shard

    def _merge_worker(self, please_stop):
        while not please_stop:
            (please_stop | Till(seconds=30)).wait()
            if not self.shards:
                continue

            while not please_stop:
                shards = list(sort_using_key(
                    [(c.bytes(), c) for c in self.shards], lambda s: s[0]
                ))
                scale = floor(log(shards[0][0]) / log(3))
                min_size = pow(3, scale)
                max_size = min_size * MERGE_RATIO
                if any(b >= max_size for b, _ in shards[0:MERGE_RATIO]):
                    break

                old_shards = self.shards[0:MERGE_RATIO]
                merged = self._merge(*old_shards)
                with self.shard_locker:
                    # DROP OLD CLUSTERS
                    new_dir = merged.dir.parent / merged.dir.name[len(IGNORE_PREFIX) :]
                    self.shards = [Shard(new_dir)] + self.shards[MERGE_RATIO:]
                    os.rename(merged.dir.abs_path, new_dir.abs_path)
                    # START DANGER - RESTART WILL SCAN THIS dir WITH EXTRA CLUSTERS

                with Timer("delete old shards"):
                    to_delete = []
                    for c in old_shards:
                        c.db.close()
                        temp = c.dir.parent / (IGNORE_PREFIX + c.dir.name)
                        to_delete.append(temp)
                        os.rename(c.dir.abs_path, temp)
                    for d in to_delete:
                        d.delete()
                    # END DANGER

    def delete(self):
        self.dir.delete()

    def query(self, query) -> Shard:
        """
        SIMPLE AGGREGATE OVER SOME SUBSET, GROUPED BY SOME OTHER COLUMNS
        :param query:
        :return: shard
        """


        return Null


    def get_document(self, _id):
        """
        RETURN DOCUMENT WITH GIVEN UUID
        """

        # BROADCAST QUERY
        result=[]
        for c in self.shards:
            result.append(c.get_document)
        result.append(self.active_shard.query(query))
        return result

        # AGGREGATE RESULT
        if query['from']!=self.name:
            Log.error("please fix me")




    def matmul(self, a, b) -> Shard:
        """
        ASSUMING a AND b REFER TO MULTIDIMENSIONAL ARRAYS
        :return: MATRIX MULTIPLY IN A CLUSTER
        """
