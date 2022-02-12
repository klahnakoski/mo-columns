# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_math import randoms

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
    sql_create,
    sql_insert,
    SQL_WHERE,
    sql_eq, sql_call, SQL_GROUPBY, SQL_COMMA,
)
from jx_sqlite.utils import (
    UID,
    GUID,
    sqlite_type_to_type_key,
    PARENT,
    ORDER,
)
from mo_columns.utils import json_type_key_to_sqlite_type, uuid
from mo_dots import concat_field, split_field, from_data, Null, is_data, tail_field, join_field, literal_field
from mo_dots.datas import leaves, Data
from mo_files import File
from mo_json import value2json
from mo_json.types import T_JSON, to_json_type, python_type_to_json_type_key, _A
from mo_logs import Log
from mo_threads import Thread, Lock
from mo_times import Timer

DEBUG = True
ID_COLUMN = concat_field(_A, GUID, python_type_to_json_type_key[str])
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
        self.columns_locker = Lock()
        self.columns = {}
        for c in dir.children:
            self.schema |= to_json_type(c.name)
            self.columns[c.name] = Sqlite(c)

    def get_next_id(self):
        try:
            return self.next_id
        finally:
            self.next_id += 1

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
            db.stop()

    def delete(self):
        self.dir.delete()

    def insert_using_json(self, documents):
        name = randoms.base64(10, "-_")
        file = self.dir / (name + ".sqlite")
        source = Sqlite(filename=file)

        with Timer("load data"):
            # CREATE TEMP TABLE
            source.query("PRAGMA synchronous = OFF")
            source.query("PRAGMA journal_mode = OFF")
            with source.transaction() as t:
                t.execute(sql_create(
                    name, {"rowid": "INTEGER", "data": "text"}, ["rowid"]
                ))

            # LOAD WITH DATA
            for g, docs in chunk(documents, 1000):
                with source.transaction() as t:
                    t.execute(sql_insert(
                        name,
                        [
                            {"rowid": self.get_next_id(), "data": value2json(add_id(d))}
                            for d in docs
                        ],
                    ))
                DEBUG and Log.note("Inserted {{rows}} rows", rows=len(docs))

        # USE JSONI TO TRANSFORM TO COLUMNS
        table_name = str(quote_column(name))
        with Timer("get columns"):
            columns = source.query(
                f"""
                WITH RECURSIVE split(path, type) AS (
                    SELECT fullkey, type
                    FROM {table_name}, json_tree({table_name}.data)
                    WHERE json_tree.type != 'object'
                    UNION ALL
                    SELECT
                        SUBSTR(path, 1, INSTR(path, '[') - 1) || '.~a~' || SUBSTR(path, INSTR(path, ']') + 1 ),
                        type
                    FROM split
                    WHERE INSTR(path, '[') 
                ) 
                SELECT path, type 
                FROM split 
                WHERE NOT INSTR(path, '[') 
                GROUP BY path, type
            """
            )
            source.stop()

        def extract_column(path, type, please_stop):
            full_path = concat_field("." if path=="$" else _A+path[1:], sqlite_type_to_type_key[type.upper()])
            key_columns = list(get_key_columns(full_path))
            column_names = list(map(quote_column, key_columns))

            dims = full_path.count(_A)
            selects = [quote_column("rowid")]
            for d in range(dims - 1):
                selects.append(SQL(f"json_extract(keys, '$[{d}]')"))

            table_name = str(quote_column("db0", name))
            rowid = quote_column("db0", name, "rowid")

            # MAKE ID COLUMN IF NOT EXISTS
            parent_full_path = full_path[:full_path.rfind(_A)]+_A
            parent_path = "$"+parent_full_path[3:]

            with self.columns_locker:
                db = self.columns.get(parent_full_path)
            if db is None:
                # MAKE ID COLUMN TABLE (ROW EXISTS)
                with self.columns_locker:
                    db = self.columns[parent_full_path] = Sqlite(
                        self.dir / concat_field(parent_full_path, "sqlite")
                    )
                self._add_exists(parent_full_path, db)

                with db.transaction() as t:
                    t.execute(ConcatSQL(
                        SQL("ATTACH "),
                        quote_value(file.abspath),
                        SQL_AS,
                        quote_column("db0"),
                    ))
                    t.execute(str(ConcatSQL(
                        SQL_INSERT,
                        quote_column(parent_full_path),
                        sql_iso(sql_list(column_names)),
                        SQL(
                            f"""
                            WITH RECURSIVE split(rowid, path, keys, atom, type) AS (
                                SELECT {rowid}, fullkey, json_array(), atom, type
                                FROM {table_name}, json_tree({table_name}.data)
                                WHERE LENGTH(fullkey)-LENGTH(REPLACE(fullkey, '[', '')) == {dims-1}
                                UNION ALL   
                                SELECT
                                    rowid,
                                    SUBSTR(path, 1, INSTR(path, '[') - 1) || '.~a~' || SUBSTR(path, INSTR(path, ']') + 1 ),      
                                    json_insert(keys, '$[#]', CAST(SUBSTR(path, INSTR(path,'[')+1, INSTR(path, ']') - INSTR(path, '[') - 1) AS INTEGER)),
                                    atom,
                                    type
                                FROM 
                                    split
                                WHERE
                                    INSTR(path, '[') )
                        """
                        ),
                        SQL_SELECT,
                        sql_list(selects),
                        SQL_FROM,
                        quote_column("split"),
                        SQL_WHERE,
                        sql_call("INSTR", quote_column("path"), quote_value(parent_path)),
                        SQL_EQ,
                        SQL_ONE,
                        SQL_GROUPBY,
                        quote_column("rowid"),
                        SQL_COMMA,
                        quote_column("keys")
                    )))
                db.query("VACUUM")

            if type == 'array':
                return

            # CREATE DATABASE FOR COLUMN
            with self.columns_locker:
                db = self.columns[full_path] = Sqlite(
                    self.dir / concat_field(full_path, "sqlite")
                )
            self._add_column(full_path, db)

            with db.transaction() as t:
                column_names.append(quote_column("value"))
                selects.append(quote_column("atom"))

                t.execute(ConcatSQL(
                    SQL("ATTACH "),
                    quote_value(file.abspath),
                    SQL_AS,
                    quote_column("db0"),
                ))
                t.execute(str(ConcatSQL(
                    SQL_INSERT,
                    quote_column(full_path),
                    sql_iso(sql_list(column_names)),
                    SQL(
                        f"""
                        WITH RECURSIVE split(rowid, path, keys, atom, type) AS (
                            SELECT {rowid}, fullkey, json_array(), atom, type
                            FROM {table_name}, json_tree({table_name}.data)
                            WHERE {str(sql_eq(type=type))} AND LENGTH(fullkey)-LENGTH(REPLACE(fullkey, '[', '')) == {quote_value(dims-1)}
                            UNION ALL   
                            SELECT
                                rowid,
                                SUBSTR(path, 1, INSTR(path, '[') - 1) || '.~a~' || SUBSTR(path, INSTR(path, ']') + 1 ),      
                                json_insert(keys, '$[#]', CAST(SUBSTR(path, INSTR(path,'[')+1, INSTR(path, ']') - INSTR(path, '[') - 1) AS INTEGER)),
                                atom,
                                type
                            FROM 
                                split
                            WHERE
                                INSTR(path, '[') )
                    """
                    ),
                    SQL_SELECT,
                    sql_list(selects),
                    SQL_FROM,
                    quote_column("split"),
                    SQL_WHERE,
                    sql_eq(type=type, path=path),
                )))
            db.query("VACUUM")

        for path, type in columns.data:
            extract_column(path, type, False)

        # threads = [
        #     Thread.run(f"{path}", extract_column, path, type)
        #     for path, type in columns.data
        # ]
        # for t in threads:
        #     t.join()

        Thread.run("delete " + file.abspath, delete_file, file)

    def insert(self, documents):
        """
        SLOWER THEN OTHER INSERT BECAUSE PYTHON DOES TOO MUCH OF THE WORK
        """
        column_values = {_A: []}

        def _add(parent_path, key, doc):
            column_values[parent_path].append(key)
            if is_data(doc):
                for path, value in leaves(doc):
                    type_key = python_type_to_json_type_key[type(value)]
                    full_path = concat_field(parent_path, path, type_key)
                    if type_key is _A:
                        column_values.setdefault(full_path, [])
                        for i, d in enumerate(value):
                            _add(full_path, key + (i,), d)
                    else:
                        column_values.setdefault(full_path, []).append((key, value))
            else:
                type_key = python_type_to_json_type_key[type(doc)]
                full_path = concat_field(parent_path, type_key)
                if type_key is _A:
                    column_values.setdefault(full_path, [])
                    for i, d in enumerate(doc):
                        _add(full_path, key + (i,), d)
                else:
                    column_values.setdefault(full_path, []).append((key, doc))

        for g, docs in chunk(documents, 1000):
            for d in docs:
                d = from_data(d)
                doc_id = (self.get_next_id(),)

                _id = d.get(GUID)
                if not _id:
                    _add(_A, doc_id, d)
                    _id = uuid()
                else:
                    del d[GUID]
                    _add(_A, doc_id, d)
                    d[GUID] = _id
                    _id = str(_id)

                column_values.setdefault(ID_COLUMN, []).append((doc_id, _id))

            for full_path, data in column_values.items():
                db = self.columns.get(full_path)
                if full_path.endswith(_A):
                    if db is None:
                        # CREATE DATABASE
                        db = self.columns[full_path] = Sqlite(
                            self.dir / concat_field(full_path, "sqlite")
                        )
                        self._add_exists(full_path, db)
                    acc = sql_list((
                        sql_iso(sql_list([quote_value(c) for c in k]))
                        for k in data
                    ))
                    columns = sql_iso(sql_list(
                        list(map(quote_column, get_key_columns(full_path)))
                    ))
                else:
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
                        list(map(quote_column, get_key_columns(full_path)))
                        + [quote_column("value")]
                    ))
                with db.transaction() as t:
                    t.execute(str(ConcatSQL(
                        SQL_INSERT, quote_column(full_path), columns, SQL_VALUES, acc
                    )))
            DEBUG and Log.note(
                "Inserted {{rows}} rows {{cols}} columns",
                rows=len(docs),
                cols=len(column_values),
            )
            column_values = {_A: []}
        return self

    def _add_column(self, path, db):
        with db.transaction() as t:
            sqlite_type = json_type_key_to_sqlite_type[split_field(path)[-1]]
            t.execute(sql_create(
                path,
                {**{k: "INTEGER" for k in get_key_columns(path)}, "value": sqlite_type},
                list(get_key_columns(path)),
            ))

            # CREATE INDEX
            key_columns = list(map(quote_column, get_key_columns(path)))
            key_list = sql_list(key_columns)
            t.execute(
                f"""CREATE INDEX {quote_column(f"index_{path}")} ON {quote_column(path)} (value, {key_list})"""
            )

    def _add_exists(self, path, db):
        """
        TABLE TO TRACK THE EXISTING NESTED OBJECTS, NOT VALUES
        """
        with db.transaction() as t:
            t.execute(sql_create(
                path,
                {**{k: "INTEGER" for k in get_key_columns(path)}},
                list(get_key_columns(path)),
            ))

    def to_rows(self, name) -> Container:
        """
        COPY COLUMNAR CLUSTER TO ROW-BASED DATABASE
        """
        file = (WORKSPACE_DIR / f"{name}.sqlite").delete()
        result_db = Sqlite(filename=file)
        pre_tables = {}
        temp_name = "__temp__"
        attachments = []

        # MAP KEY COLUMNS TO NEW rowid
        for i, (path, db) in enumerate(self.columns.items()):
            if not path.endswith(_A):
                continue

            file = self.dir / concat_field(path, "sqlite")
            db_alias = f"db{i}"
            attachments.append(ConcatSQL(
                SQL("ATTACH "), sql_alias(quote_value(file.abspath), db_alias)
            ))

            table_alias = f"c{i}"
            parent_path = "."
            table_path = "."
            acc = ""
            for prefix in path.split(_A)[:-1]:
                parent_path = table_path
                acc = acc + prefix + _A
                table_path = acc

            if table_path == _A:
                pre_tables[table_path] = Data(
                    dest=concat_field(temp_name, table_path),
                    selects=[sql_alias(quote_column(table_alias, UID), UID)],
                    frum=sql_alias(quote_column(db_alias, path), table_alias),
                    name=concat_field(name, path),
                    alias=table_alias,
                    joins=[],
                )
            else:
                parent = pre_tables[parent_path]
                key_columns = list(get_key_columns(table_path))
                parent_keys, order = key_columns[:-1], key_columns[-1]

                pre_tables[table_path] = Data(
                    dest=concat_field(temp_name, table_path),
                    selects=[
                        sql_alias(quote_column(parent.alias, "rowid"), PARENT),
                        sql_alias(quote_column(table_alias, order), ORDER),
                        *(quote_column(table_alias, k) for k in key_columns),
                    ],
                    frum=sql_alias(quote_column(parent.dest), parent.alias),
                    name=concat_field(name, path),
                    alias=table_alias,
                    joins=[ConcatSQL(
                        sql_alias(quote_column(db_alias, table_path), table_alias),
                        SQL_ON,
                        JoinSQL(
                            SQL_AND,
                            [
                                ConcatSQL(
                                    quote_column(table_alias, k),
                                    SQL_EQ,
                                    quote_column(parent.alias, k),
                                )
                                for k in parent_keys
                            ],
                        ),
                    )],
                )

        with result_db.transaction() as t:
            for a in attachments:
                t.execute(a)
            for path, table in pre_tables.items():
                t.execute(ConcatSQL(
                    SQL_CREATE,
                    quote_column(table.dest),
                    SQL_AS,
                    SQL_SELECT,
                    sql_list(table.selects),
                    SQL_FROM,
                    table.frum,
                    ConcatSQL(*(ConcatSQL(SQL_LEFT_JOIN, j) for j in table.joins)),
                ))

        tables = {}
        for path, table in pre_tables.items():
            dest = join_field([name] + split_field(path)[1:])
            if path == _A:
                columns = {UID: "INTEGER"}
                selects = [
                    sql_alias(quote_column(table.alias, "rowid"), UID)
                ]
            else:
                columns = {
                    UID: "INTEGER",
                    PARENT: "INTEGER",
                    ORDER: "INTEGER"
                }
                selects = [
                    sql_alias(quote_column(table.alias, "rowid"), UID),
                    quote_column(table.alias, PARENT),
                    quote_column(table.alias, ORDER),
                ]

            tables[path] = Data(
                dest=dest,
                columns=columns,
                selects=selects,
                frum=sql_alias(quote_column(table.dest), table.alias),
                name=concat_field(name, path),
                alias=table.alias,
                joins=[],
            )

        # JOIN VALUES TO TABLES
        attachments = []
        for i, (path, db) in enumerate(self.columns.items()):
            if path.endswith(_A):
                continue

            file = self.dir / concat_field(path, "sqlite")
            db_alias = f"db{i}"
            attachments.append(ConcatSQL(
                SQL("ATTACH "), sql_alias(quote_value(file.abspath), db_alias)
            ))

            table_alias = f"c{i}"
            parent_path = path[:path.rfind(_A)] + _A
            parent = tables[parent_path]
            column_name = tail_field(path)[1]
            parent.selects.append(sql_alias(
                quote_column(table_alias, "value"), column_name
            ))
            parent.columns[literal_field(column_name)] = json_type_key_to_sqlite_type[split_field(column_name)[-1]]

            table_name = quote_column(db_alias, path)
            key_columns = get_key_columns(path)
            parent.joins.append(ConcatSQL(
                sql_alias(table_name, table_alias),
                SQL_ON,
                JoinSQL(
                    SQL_AND,
                    [
                        ConcatSQL(
                            quote_column(table_alias, k),
                            SQL_EQ,
                            quote_column(pre_tables[parent_path].alias, k),
                        )
                        for k in key_columns
                    ],
                ),
            ))

        with result_db.transaction() as t:
            for a in attachments:
                t.execute(a)
            for path, table in tables.items():
                t.execute(sql_create(table.dest, table.columns, [UID]))
                t.execute(ConcatSQL(
                    SQL_INSERT,
                    quote_column(table.dest),
                    SQL_SELECT,
                    sql_list(table.selects),
                    SQL_FROM,
                    table.frum,
                    ConcatSQL(*(ConcatSQL(SQL_LEFT_JOIN, j) for j in table.joins)),
                ))

        return Container(db=result_db)

    def query(self, query):
        norm = QueryOp.define(query)

        # OPEN NEW DATABSE
        # ATTACHED REQUIRED COLUMNS
        # PROCESS WHERE CLAUSE, FINDING DOCS
        # SELECT COLUMNS
        return Null


def add_id(d):
    """
    ENSURE doc HAS _id
    """
    _id = d.get(GUID)
    if _id:
        return d
    else:
        return {"_id": uuid(), **d}


def get_key_columns(path):
    """
    RETURN A TUPLE OF KEYS REQUIRED TO STORE GIVEN PATH
    """
    yield UID
    for i in range(1, path.count(_A)):
        yield f"_id{i}"


def delete_file(file, please_stop):
    while True:
        try:
            file.delete()
            break
        except Exception:
            pass
