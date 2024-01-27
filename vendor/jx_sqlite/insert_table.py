# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from typing import Dict, List
from uuid import uuid4

from jx_sqlite.utils import untyped_column

<<<<<<< .mine
from __future__ import absolute_import, division, unicode_literals

from typing import Dict, List

from jx_base import Column, generateGuid, Facts
||||||| .r1729


from typing import Dict, List

from jx_base import generateGuid, Facts
=======
from jx_base import Column, Facts
>>>>>>> .r2071
from jx_base.expressions import jx_expression, TRUE
<<<<<<< .mine
from jx_sqlite.expressions._utils import json_type_to_sql_type_key
from jx_sqlite.sqlite import (
||||||| .r1729
from jx_base.meta_columns import Column
from jx_base.models.nested_path import NestedPath
from mo_sqlite import (
=======
from mo_sql.utils import json_type_to_sql_type_key
from mo_sqlite import (
>>>>>>> .r2071
    SQL_AND,
    SQL_FROM,
    SQL_INNER_JOIN,
    SQL_NULL,
    SQL_SELECT,
    SQL_TRUE,
    SQL_UNION_ALL,
    SQL_WHERE,
    sql_iso,
    sql_list,
    SQL_VALUES,
    SQL_INSERT,
    ConcatSQL,
    SQL_EQ,
    SQL_UPDATE,
    SQL_SET,
    SQL_ONE,
    SQL_DELETE,
    SQL_ON,
    SQL_COMMA,
)
from jx_sqlite.sqlite import (
    json_type_to_sqlite_type,
    quote_column,
    quote_value,
    sql_alias,
)
from jx_sqlite.utils import (
    GUID,
    ORDER,
    PARENT,
    UID,
    get_if_type,
<<<<<<< .mine
    value_to_jx_type,
    typed_column,
    untyped_column,
||||||| .r1729
    value_to_json_type,
=======
    typed_column,
>>>>>>> .r2071
)
from mo_dots import (
    Data,
    Null,
    concat_field,
    listwrap,
    startswith_field,
    unwrap,
    is_many,
    is_data,
    to_data,
)
from mo_future import text, first
from mo_json import STRUCT, ARRAY, OBJECT, value_to_json_type
from mo_logs import Log
from mo_times import Date


class InsertTable(Facts):
    def add(self, doc):
        self.insert([doc])

    def insert(self, docs):
        if not is_many(docs):
            Log.error("Expecting a list of documents")
        doc_collection = self.flatten_many(docs)
        self._insert(doc_collection)

    def update(self, command):
        """
        :param command:  EXPECTING dict WITH {"set": s, "clear": c, "where": w} FORMAT
        """
        command = to_data(command)
        clear_columns = set(listwrap(command["clear"]))

        # REJECT DEEP UPDATES
        touched_columns = command.set.keys() | clear_columns
        for c in self.schema.columns:
            if c.name in touched_columns and len(c.nested_path) > 1:
                Log.error("Deep update not supported")

        # ADD NEW COLUMNS
        where = jx_expression(command.where) or TRUE
        _vars = where.vars()
<<<<<<< .mine
        _map = {
            v: c.es_column
            for v in _vars
            for c in self.columns.get(v, Null)
            if c.jx_type not in STRUCT
        }
||||||| .r1729
        _map = {
            v: c.es_column
            for v in _vars
            for c in self.columns.get(v, Null)
            if c.json_type not in STRUCT
        }
=======
        _map = {v: c.es_column for v in _vars for c in self.columns.get(v, Null) if c.json_type not in STRUCT}
>>>>>>> .r2071
        where_sql = where.map(_map).to_sql(self.schema)
        new_columns = set(command.set.keys()) - set(c.name for c in self.schema.columns)
        for new_column_name in new_columns:
            nested_value = command.set[new_column_name]
            ctype = value_to_jx_type(nested_value)
            column = Column(
                name=new_column_name,
                jx_type=ctype,
                es_index=self.name,
                es_type=json_type_to_sqlite_type(ctype),
                es_column=typed_column(new_column_name, ctype),
                last_updated=Date.now(),
            )
            self.add_column(column)

        # UPDATE THE ARRAY VALUES
        for nested_column_name, nested_value in command.set.items():
            if value_to_jx_type(nested_value) == "nested":
                nested_table_name = concat_field(self.name, nested_column_name)
                nested_table = nested_tables[nested_column_name]
                self_primary_key = sql_list(quote_column(c.es_column) for u in self.uid for c in self.columns[u])
                extra_key_name = UID + text(len(self.uid))
                extra_key = [e for e in nested_table.columns[extra_key_name]][0]

                sql_command = ConcatSQL(
                    SQL_DELETE,
                    SQL_FROM,
                    quote_column(nested_table.name),
                    SQL_WHERE,
                    "EXISTS",
                    sql_iso(
                        SQL_SELECT,
                        SQL_ONE,
                        SQL_FROM,
                        sql_alias(quote_column(nested_table.name), "n"),
                        SQL_INNER_JOIN,
                        sql_iso(
                            SQL_SELECT,
                            self_primary_key,
                            SQL_FROM,
                            quote_column(abs_schema.fact),
                            SQL_WHERE,
                            where_sql,
                        ),
                        quote_column("t"),
                        SQL_ON,
                        SQL_AND.join(
                            ConcatSQL(quote_column("t", c.es_column), SQL_EQ, quote_column("n", c.es_column),)
                            for u in self.uid
                            for c in self.columns[u]
                        ),
                    ),
                )
                self.container.db.execute(sql_command)

                # INSERT NEW RECORDS
                if not nested_value:
                    continue

                doc_collection = {}
                for d in listwrap(nested_value):
                    nested_table.flatten(d, Data(), doc_collection, path=nested_column_name)

                prefix = ConcatSQL(
                    SQL_INSERT,
                    quote_column(nested_table.name),
                    sql_iso(sql_list(
                        [self_primary_key]
                        + [quote_column(extra_key)]
                        + [quote_column(c.es_column) for c in doc_collection["."].active_columns]
                    )),
                )

                # BUILD THE PARENT TABLES
                parent = ConcatSQL(
                    SQL_SELECT,
                    self_primary_key,
                    SQL_FROM,
                    quote_column(abs_schema.fact),
                    SQL_WHERE,
                    jx_expression(command.where).to_sql(schema),
                )

                # BUILD THE RECORDS
                children = SQL_UNION_ALL.join(
                    ConcatSQL(
                        SQL_SELECT,
                        sql_alias(quote_value(i), extra_key.es_column),
                        SQL_COMMA,
                        sql_list(
                            sql_alias(quote_value(row[c.name]), quote_column(c.es_column))
                            for c in doc_collection["."].active_columns
                        ),
                    )
                    for i, row in enumerate(doc_collection["."].rows)
                )

                sql_command = ConcatSQL(
                    prefix,
                    SQL_SELECT,
                    sql_list(
                        [quote_column("p", c.es_column) for u in self.uid for c in self.columns[u]]
                        + [quote_column("c", extra_key)]
                        + [quote_column("c", c.es_column) for c in doc_collection["."].active_columns]
                    ),
                    SQL_FROM,
                    sql_iso(parent),
                    quote_column("p"),
                    SQL_INNER_JOIN,
                    sql_iso(children),
                    quote_column("c"),
                    SQL_ON,
                    SQL_TRUE,
                )

                self.container.db.execute(sql_command)

                # THE CHILD COLUMNS COULD HAVE EXPANDED
                # ADD COLUMNS TO SELF
                for n, cs in nested_table.columns.items():
                    for c in cs:
                        column = Column(
                            name=c.name,
                            jx_type=c.jx_type,
                            es_type=c.es_type,
                            es_index=c.es_index,
                            es_column=c.es_column,
                            nested_path=[nested_column_name] + c.nested_path,
                            last_updated=Date.now(),
                        )
                        if c.name not in self.columns:
                            self.columns[column.name] = {column}
                        elif c.jx_type not in [c.jx_type for c in self.columns[c.name]]:
                            self.columns[column.name].add(column)

        command = ConcatSQL(
            SQL_UPDATE,
            quote_column(self.name),
            SQL_SET,
            sql_list(
                [
<<<<<<< .mine
                    ConcatSQL(
                        quote_column(c.es_column),
                        SQL_EQ,
                        quote_value(get_if_type(v, c.jx_type)),
                    )
||||||| .r1729
                    ConcatSQL(
                        quote_column(c.es_column),
                        SQL_EQ,
                        quote_value(get_if_type(v, c.json_type)),
                    )
=======
                    ConcatSQL(quote_column(c.es_column), SQL_EQ, quote_value(get_if_type(v, c.json_type)),)
>>>>>>> .r2071
                    for c in self.schema.columns
                    if c.jx_type != ARRAY and len(c.nested_path) == 1
                    for v in [command.set[c.name]]
                    if v != None
                ]
                + [
                    ConcatSQL(quote_column(c.es_column), SQL_EQ, SQL_NULL)
                    for c in self.schema.columns
                    if (
                        c.name in clear_columns
                        and command.set[c.name] != None
                        and c.jx_type != ARRAY
                        and len(c.nested_path) == 1
                    )
                ]
            ),
            SQL_WHERE,
            where_sql,
        )

        with self.container.db.transaction() as t:
            t.execute(command)

    def upsert(self, doc, where):
        self.delete(where)
        self.insert([doc])

    def flatten_many(self, docs):
        """
        :param docs: THE JSON DOCUMENTS
        :return: TUPLE (success, command, doc_collection) WHERE
                 success: BOOLEAN INDICATING PROPER PARSING
                 command: SCHEMA CHANGES REQUIRED TO BE SUCCESSFUL NEXT TIME
                 doc_collection: MAP FROM NESTED PATH TO INSERTION PARAMETERS:
                 {"active_columns": list, "rows": list of objects}
        """

        facts_insertion = Insertion()
        doc_collection: Dict[str, Insertion] = {self.name: facts_insertion}
        # KEEP TRACK OF WHAT TABLE WILL BE MADE (SHORTLY)
        required_changes = []
        snowflake = self.container.get_or_create_facts(self.name).snowflake

        def _flatten(doc, doc_path, nested_path, row, row_num, row_id, parent_id):
            """
            :param doc: the data we are pulling apart
            :param doc_path: path to this (sub)doc
            :param nested_path: list of paths, deepest first, pointing to table
            :param row: we will be filling this
            :param row_num: the number of siblings before this one
            :param row_id: the id we are giving this row
            :param parent_id: the parent id of this (sub)doc
            :return:
            """
<<<<<<< .mine
            curr_query_path = nested_path[0]
            table_name = concat_field(self.name, curr_query_path)
            insertion = doc_collection.setdefault(curr_query_path, Insertion())
||||||| .r1729
            table_name = nested_path[0]
            curr_query_path = relative_field(table_name, self.name)
            insertion = doc_collection.setdefault(curr_query_path, Insertion())
=======
            table_name = nested_path[0]
            insertion = doc_collection.setdefault(table_name, Insertion())
>>>>>>> .r2071

            if is_data(doc):
                items = [(k, v) for k, v in to_data(doc).leaves()]
            else:
                # PRIMITIVE VALUES
                items = [(".", doc)]

            for rel_name, v in items:
<<<<<<< .mine
                abs_name = concat_field(doc_path, rel_name)
                jx_type = value_to_jx_type(v)
                if jx_type is None:
||||||| .r1729
                abs_name = concat_field(nested_path[0], rel_name)
                json_type = value_to_json_type(v)
                if json_type is None:
=======
                abs_name = concat_field(doc_path, rel_name)
                json_type = value_to_json_type(v)
                if json_type is None:
>>>>>>> .r2071
                    continue

                columns = snowflake.get_schema(nested_path).columns + insertion.active_columns
                if jx_type == ARRAY:
                    curr_column = first(
<<<<<<< .mine
                        cc
                        for cc in columns
                        if cc.jx_type in STRUCT
                        and untyped_column(cc.name)[0] == abs_name
||||||| .r1729
                        cc
                        for cc in columns
                        if cc.json_type in STRUCT
                        and untyped_column(cc.name)[0] == abs_name
=======
                        cc for cc in columns if cc.json_type in STRUCT and untyped_column(cc.name)[0] == abs_name
>>>>>>> .r2071
                    )
                    if curr_column:
                        deeper_insertion = doc_collection.setdefault(concat_field(curr_column.es_index, curr_column.es_column), Insertion())

                else:
<<<<<<< .mine
                    curr_column = first(
                        cc
                        for cc in columns
                        if cc.jx_type == jx_type and cc.name == abs_name
                    )
||||||| .r1729
                    curr_column = first(
                        cc
                        for cc in columns
                        if cc.json_type == json_type and cc.name == abs_name
                    )
=======
                    curr_column = first(cc for cc in columns if cc.json_type == json_type and cc.name == abs_name)
>>>>>>> .r2071

                if not curr_column:
                    # WHAT IS THE NESTING LEVEL FOR THIS PATH?
                    deeper_nested_path = "."
                    for path in snowflake.query_paths + insertion.query_paths:
                        if startswith_field(abs_name, path[0]) and len(deeper_nested_path) < len(path):
                            deeper_nested_path = path

                    curr_column = Column(
                        name=abs_name,
                        jx_type=jx_type,
                        es_type=json_type_to_sqlite_type.get(jx_type, jx_type),
                        es_column=typed_column(
<<<<<<< .mine
                            concat_field(nested_path[0], rel_name),
                            json_type_to_sql_type_key.get(jx_type),
||||||| .r1729
                            rel_name,
                            json_type_to_sql_type_key.get(json_type),
=======
                            concat_field(relative_field(nested_path[0], table_name), rel_name),
                            json_type_to_sql_type_key.get(json_type),
>>>>>>> .r2071
                        ),
                        es_index=table_name,
                        cardinality=0,
                        multi=1,
                        nested_path=nested_path,
                        last_updated=Date.now(),
                    )
                    if jx_type == ARRAY:
                        # NOTE: ADVANCE active_columns TO THIS NESTED LEVEL
                        # SCHEMA (AND DATABASE) WILL BE UPDATED LATER
                        new_query_path = concat_field(curr_column.es_index, curr_column.es_column)
                        deeper_insertion = doc_collection.setdefault(new_query_path, Insertion())
                        old_column_prefix, _ = untyped_column(curr_column.es_column)
                        for c in list(insertion.active_columns):
                            if c.nested_path[0] == nested_path[0] and startswith_field(c.es_column, old_column_prefix):
                                doc_collection[table_name].active_columns.remove(c)
                                doc_collection[new_query_path].active_columns.append(c)
                        insertion.query_paths.append(curr_column.es_column)
                        required_changes.append({"nest": curr_column})
                    else:
                        required_changes.append({"add": curr_column})

                    insertion.active_columns.append(curr_column)

                elif curr_column.jx_type == ARRAY and jx_type == OBJECT:
                    # ALWAYS PROMOTE OBJECTS TO NESTED
                    jx_type = ARRAY
                    v = [v]
                elif len(curr_column.nested_path) < len(nested_path):
                    es_column = curr_column.es_column
                    # # required_changes.append({"nest": c})
                    # deeper_column = Column(
                    #     name=abs_name,
                    #     jx_type=jx_type,
                    #     es_type=json_type_to_sqlite_type.get(jx_type, jx_type),
                    #     es_column=typed_column(
                    #         abs_name, json_type_to_sql_key.get(jx_type)
                    #     ),
                    #     es_index=table_name,
                    #     nested_path=nested_path,
                    #     last_updated=Date.now(),
                    #     multi=1
                    # )
                    # insertion.active_columns.remove(curr_column)
                    # insertion.active_columns.append(deeper_column)

                    # PROMOTE COLUMN TO ARRAY OF VALUES
                    parent_rows = doc_collection[table_name].rows
                    for r in parent_rows:
                        if es_column in r:
                            deeper_es_column = typed_column(
<<<<<<< .mine
                                concat_field(nested_path[0], rel_name),
                                json_type_to_sql_type_key.get(jx_type),
||||||| .r1729
                                concat_field(nested_path[0], rel_name),
                                json_type_to_sql_type_key.get(json_type),
=======
                                concat_field(nested_path[0], rel_name), json_type_to_sql_type_key.get(json_type),
>>>>>>> .r2071
                            )

                            row1 = {
                                UID: self.container.next_uid(),
                                PARENT: r[UID],
                                ORDER: 0,
                                deeper_es_column: r[es_column],
                            }
                            insertion.rows.append(row1)
                elif len(curr_column.nested_path) > len(nested_path):
                    insertion = doc_collection[curr_column.nested_path[0]]
                    row = {
                        UID: self.container.next_uid(),
                        PARENT: row_id,
                        ORDER: row_num,
                    }
                    insertion.rows.append(row)

                # BE SURE TO NEST VALUES, IF NEEDED
                if jx_type == ARRAY:
                    for child_row_num, child_data in enumerate(v):
                        child_uid = self.container.next_uid()
                        child_row = {
                            UID: child_uid,
                            PARENT: row_id,
                            ORDER: child_row_num,
                        }
                        deeper_insertion.rows.append(child_row)

                        _flatten(
                            doc=child_data,
                            doc_path=abs_name,
<<<<<<< .mine
                            nested_path=[curr_column.es_column] + nested_path,
||||||| .r1729
                            nested_path=[concat_field(self.name, curr_column.es_column)] + nested_path,
=======
                            nested_path=[concat_field(curr_column.es_index, curr_column.es_column), *nested_path],
>>>>>>> .r2071
                            row=child_row,
                            row_num=child_row_num,
                            row_id=child_uid,
                            parent_id=row_id,
                        )
                elif jx_type == OBJECT:
                    _flatten(
                        doc=v,
                        doc_path=abs_name,
                        nested_path=nested_path,
                        row=row,
                        row_num=row_num,
                        row_id=row_id,
                        parent_id=parent_id,
                    )
                elif curr_column.jx_type:
                    row[curr_column.es_column] = v

        for doc in docs:
            uid = self.container.next_uid()
            row = {GUID:  str(uuid4()), UID: uid}
            facts_insertion.rows.append(row)
            _flatten(
<<<<<<< .mine
                doc=doc,
                doc_path=".",
                nested_path=["."],
                row=row,
                row_num=0,
                row_id=uid,
                parent_id=0,
||||||| .r1729
                doc=doc,
                doc_path=".",
                nested_path=[self.name],
                row=row,
                row_num=0,
                row_id=uid,
                parent_id=0,
=======
                doc=doc, doc_path=".", nested_path=[self.name], row=row, row_num=0, row_id=uid, parent_id=0,
>>>>>>> .r2071
            )
            if required_changes:
                snowflake.change_schema(required_changes)
            required_changes = []

        return doc_collection

    def _insert(self, collection):
        for nested_path, insertion in collection.items():
<<<<<<< .mine
            column_names = [
                c.es_column for c in insertion.active_columns if c.jx_type != ARRAY
            ]
||||||| .r1729
            column_names = [
                c.es_column for c in insertion.active_columns if c.json_type != ARRAY
            ]
=======
            column_names = [c.es_column for c in insertion.active_columns if c.json_type != ARRAY]
>>>>>>> .r2071
            rows = insertion.rows
            table_name = nested_path

            if table_name == self.name:
                # DO NOT REQUIRE PARENT OR ORDER COLUMNS
                meta_columns = [GUID, UID]
            else:
                meta_columns = [UID, PARENT, ORDER]

            all_columns = tuple(meta_columns + column_names)
            command = ConcatSQL(
                SQL_INSERT,
                quote_column(table_name),
                sql_iso(sql_list(map(quote_column, all_columns))),
                SQL_VALUES,
<<<<<<< .mine
                sql_list(
                    sql_iso(sql_list(quote_value(row.get(c)) for c in all_columns))
                    for row in unwrap(rows)
                ),
||||||| .r1729
                sql_list(
                    sql_iso(sql_list(quote_value(row.get(c)) for c in all_columns))
                    for row in from_data(rows)
                ),
=======
                sql_list(sql_iso(sql_list(quote_value(row.get(c)) for c in all_columns)) for row in from_data(rows)),
>>>>>>> .r2071
            )

            with self.container.db.transaction() as t:
                t.execute(command)


class Insertion:
    def __init__(self):
        self.active_columns = []
        self.rows: List[Dict] = []
        self.query_paths: List[str] = []  # CHILDREN ARRAYS


