# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_sqlite.sqlite import Sqlite, quote_column, quote_value
from mo_dots import concat_field

from vendor.mo_files import File
from vendor.mo_json.types import T_JSON, to_json_type, python_type_to_json_type_key, _A


class Cluster(object):
    """
    A database
    """

    def __init__(self, dir: File):
        self.dir = dir
        self.name = dir.name
        self.schema = T_JSON
        self.next_id = 0
        self.columns = {}
        for c in dir.children:
            self.schema |= to_json_type(c.name)
            self.columns[c.name] = Sqlite(c)

    def bytes(self):
        """
        NUMBER OF BYTES ON DISK
        :return:
        """
        return sum(c.size for c in self.dir.children)

    def count(self):
        """
        NUMBER OF DOCUMENTS
        :return:
        """
        return self.next_id

    def open(self):
        """
        open cluster for record insertion
        :return:
        """

    def close(self):
        """
        prevent further record insertion
        :return:
        """

    def delete(self):
        self.dir.delete()

    def add(self, documents):
        column_values = {}

        def _add(parent_path, key, doc):
            for path, value in doc.leaves():
                data_key = python_type_to_json_type_key[type(value)]
                full_path = concat_field(concat_field(parent_path, path), data_key)
                if data_key is _A:
                    for i, d in enumerate(value):
                        _add(full_path, key + (i,), d)
                else:
                    column_values.get(full_path, []).append((key, value))

        for d in documents:
            doc_id = (self.next_id,)
            self.next_id += 1
            _add(".", doc_id, d)

        for full_path, data in column_values:
            db = self.columns.get(full_path)
            if db is None:
                # CREATE DATABASE
                db = self.columns[full_path] = Sqlite(full_path)
                self.add_column(len(data[0][0]), full_path, db)
            acc = ",".join(
                f"({kk}, {quote_value(v)})"
                for k, v in data
                for kk in [",".join(quote_value(c) for c in k)]
            )
            with db.transaction() as t:
                t.execute(f"INSERT INTO {quote_column(full_path)} VALUES {acc}")
        return self

    def add_column(self, path, db):
        with db.transaction() as t:
            # CREATE TABLE
            key_columns = get_key_columns(path)
            t.execute(
                f"CREATE TABLE {quote_column(path)} ({key_columns}, value"
                f" {}, PRIMARY KEY {key_columns}) WITHOUT ROWID"
            )
            # CREATE INDEX
            t.execute(
                f'CREATE INDEX {quote_column(f"index_{path}")} ON (value,'
                f" {key_columns}"
            )


def get_key_columns(path):
    """
    RETURN A TUPLE OF KEYS REQUIRED TO STORE GIVEN PATH
    """
    return tuple(
        f"_id{i} INTEGER" for i in range(path.count(_A))
    )
