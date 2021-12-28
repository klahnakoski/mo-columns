# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import QueryOp
from jx_python.jx import chunk
from jx_sqlite import Container
from jx_sqlite.sqlite import (
    Sqlite,
    quote_column,
    quote_value,
    ConcatSQL,
    SQL_INSERT,
    SQL_VALUES,
    SQL,
    SQL_CREATE,
    sql_iso,
    SQL_COMMA,
    sql_list,
    SQL_SELECT,
    sql_count,
    SQL_ONE,
    SQL_FROM,
    sql_alias,
    SQL_ON,
    JoinSQL,
    SQL_AND,
    SQL_EQ,
    SQL_AS,
    SQL_LEFT_JOIN,
)
from jx_sqlite.utils import UID, GUID
from mo_columns.utils import json_type_key_to_sqlite_type, uuid
from mo_dots import concat_field, split_field, from_data, Null
from mo_dots.datas import leaves
from mo_files import File
from mo_json.types import T_JSON, to_json_type, python_type_to_json_type_key, _A
from mo_logs import Log

ID_COLUMN = concat_field(GUID, python_type_to_json_type_key[str])
WORKSPACE_DIR = File("temp")


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
        return (
            self
            .columns[ID_COLUMN]
            .query(ConcatSQL(
                SQL_SELECT, sql_count(SQL_ONE), SQL_FROM, quote_column(ID_COLUMN)
            ))
            .data[0][0]
        )

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
        for path, db in self.columns.items():
            db.close()

    def delete(self):
        self.dir.delete()

    def insert(self, documents):
        column_values = {}

        def _add(parent_path, key, doc):
            for path, value in leaves(doc):
                type_key = python_type_to_json_type_key[type(value)]
                full_path = concat_field(concat_field(parent_path, path), type_key)
                if type_key is _A:
                    for i, d in enumerate(value):
                        _add(full_path, key + (i,), d)
                else:
                    column_values.setdefault(full_path, []).append((key, value))

        for g, docs in chunk(documents, 1000):
            for d in docs:
                d = from_data(d)
                doc_id = (self.next_id,)
                self.next_id += 1
                _id = d.get(GUID)
                if not _id:
                    _add(".", doc_id, d)
                    _id = uuid()
                else:
                    _add(".", doc_id, {k: v for k, v in d.items() if k != GUID})
                    _id = str(_id)

                column_values.setdefault(ID_COLUMN, []).append((doc_id, _id))

            for full_path, data in column_values.items():
                db = self.columns.get(full_path)
                if db is None:
                    # CREATE DATABASE
                    db = self.columns[full_path] = Sqlite(
                        self.dir / concat_field(full_path, "sqlite")
                    )
                    self._add_column(full_path, db)
                acc = sql_list((
                    sql_iso(sql_list((kk, quote_value(v))))
                    for k, v in data
                    for kk in [sql_list([quote_value(c) for c in k])]
                ))
                columns = sql_iso(sql_list(
                    list(map(quote_value, get_key_columns(full_path))) + [quote_value("value")]
                ))
                with db.transaction() as t:
                    t.execute(str(ConcatSQL(
                        SQL_INSERT, quote_column(full_path), columns, SQL_VALUES, acc
                    )))
            column_values = {}
        return self

    def _add_column(self, path, db):
        with db.transaction() as t:
            # CREATE TABLE
            key_columns = list(map(quote_column, get_key_columns(path)))
            key_sql = sql_list([ConcatSQL(k, SQL("INTEGER")) for k in key_columns])
            key_list = sql_list(key_columns)
            sqlite_type = json_type_key_to_sqlite_type[split_field(path)[-1]]
            t.execute(ConcatSQL(
                SQL_CREATE,
                quote_column(path),
                sql_iso(ConcatSQL(
                    key_sql,
                    SQL_COMMA,
                    quote_column("value"),
                    sqlite_type,
                    SQL_COMMA,
                    SQL("PRIMARY KEY"),
                    sql_iso(key_list),
                )),
                SQL("WITHOUT ROWID"),
            ))
            # CREATE INDEX
            t.execute(
                f"""CREATE INDEX {quote_column(f"index_{path}")} ON {quote_column(path)} (value, {key_list})"""
            )

    def find(self, where, name) -> Container:
        """
        RETURN DOC IDS THAT MATCH where CLAUSE
        """
        # MAKE CONTAINER
        file = WORKSPACE_DIR / f"{name}.sqlite"
        file.delete()
        result_db = Sqlite(filename=file)

        with result_db.transaction() as t:
            # FIND _id TABLE
            selects = []
            frum_alias = None
            frum = None
            for i, (path, db) in enumerate(self.columns.items()):
                if path == ID_COLUMN:
                    db_alias = f"db{i}"
                    table_name = quote_column(db_alias, path)
                    frum_alias = f"c{i}"
                    frum = sql_alias(table_name, frum_alias)
                    selects.append(sql_alias(quote_column(frum_alias, UID), UID))

            joins = []
            # COMPOSE
            for i, (path, db) in enumerate(self.columns.items()):
                if _A in path:
                    Log.error("not supported")

                file = self.dir / concat_field(path, "sqlite")
                db_alias = f"db{i}"
                t.execute(ConcatSQL(
                    SQL("ATTACH "), sql_alias(quote_value(file.abspath), db_alias)
                ))
                table_alias = f"c{i}"
                selects.append(sql_alias(quote_column(table_alias, "value"), path))
                if path == ID_COLUMN:
                    continue

                table_name = quote_column(db_alias, path)
                key_columns = get_key_columns(path)
                ons = JoinSQL(
                    SQL_AND,
                    [
                        ConcatSQL(
                            quote_column(table_alias, k),
                            SQL_EQ,
                            quote_column(frum_alias, k),
                        )
                        for k in key_columns
                    ],
                )
                joins.append(ConcatSQL(sql_alias(table_name, table_alias), SQL_ON, ons))

            t.execute(ConcatSQL(
                SQL_CREATE,
                quote_column(name),
                SQL_AS,
                SQL_SELECT,
                sql_list(selects),
                SQL_FROM,
                frum,
                ConcatSQL(*(ConcatSQL(SQL_LEFT_JOIN, j) for j in joins)),
            ))

        return Container(db=result_db)

    def query(self, query):
        norm = QueryOp.define(query)

        # OPEN NEW DATABSE
        # ATTACHED REQUIRED COLUMNS
        # PROCESS WHERE CLAUSE, FINDING DOCS
        # SELECT COLUMNS
        return Null


def get_key_columns(path):
    """
    RETURN A TUPLE OF KEYS REQUIRED TO STORE GIVEN PATH
    """
    yield UID
    for i in range(1, path.count(_A) + 1):
        yield f"_id{i}"
