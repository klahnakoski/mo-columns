# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
<<<<<<< .mine


from __future__ import absolute_import, division, unicode_literals

||||||| .r1729




=======
>>>>>>> .r2071
from typing import List, Dict, Tuple

<<<<<<< .mine
from mo_math import UNION

||||||| .r1729
from jx_base.meta_columns import Column
from mo_imports import export

=======
from jx_base import Column, is_op
>>>>>>> .r2071
from jx_base import Column
from jx_base.expressions import NULL
from jx_python import jx
from jx_sqlite.expressions._utils import SQLang
from jx_sqlite.expressions.leaves_op import LeavesOp
from jx_sqlite.expressions.to_boolean_op import ToBooleanOp
from jx_sqlite.insert_table import InsertTable
<<<<<<< .mine
from jx_sqlite.sqlite import (
    SQL_FROM,
    SQL_IS_NULL,
    SQL_LEFT_JOIN,
    SQL_LIMIT,
    SQL_NULL,
    SQL_ON,
    SQL_ORDERBY,
    SQL_SELECT,
    SQL_UNION_ALL,
    sql_iso,
    sql_list,
    ConcatSQL,
    SQL_STAR,
    SQL_EQ,
    SQL_DESC, SQL_WITH, SQL_AS,
)
from jx_sqlite.sqlite import quote_column, sql_alias
||||||| .r1729
from mo_sqlite import quote_column, sql_alias
=======
>>>>>>> .r2071
from jx_sqlite.utils import (
    COLUMN,
    ColumnMapping,
    ORDER,
    _make_column_name,
    get_column,
    UID,
    PARENT,
    table_alias,
    untype_field, sort_to_sqlite_order,
)
from mo_dots import (
    Data,
    startswith_field,
    unwraplist,
    relative_field,
    is_missing,
    Null,
    tail_field,
    literal_field,
    unliteral_field, list_to_data,
)
from mo_future import text
<<<<<<< .mine
from mo_json.types import json_type_to_simple_type, OBJECT
||||||| .r1729
from mo_json.types import jx_type_to_json_type, OBJECT
=======
from mo_json.types import OBJECT, jx_type_to_json_type
>>>>>>> .r2071
from mo_logs import Log
<<<<<<< .mine
||||||| .r1729
from mo_math import UNION
from mo_sql.utils import untype_field
=======
from mo_sqlite import (
    SQL_AND,
    SQL_FROM,
    SQL_IS_NULL,
    SQL_LEFT_JOIN,
    SQL_LIMIT,
    SQL_NULL,
    SQL_ON,
    SQL_ORDERBY,
    SQL_SELECT,
    SQL_UNION_ALL,
    SQL_WHERE,
    sql_iso,
    sql_list,
    ConcatSQL,
    SQL_STAR,
    SQL_EQ,
    SQL_ZERO,
    SQL_GT,
)
from mo_sqlite import quote_column, sql_alias
>>>>>>> .r2071
from mo_times import Date


class SetOpTable(InsertTable):
    def _set_op(self, query):
        index_to_column, ordered_sql, primary_doc_details = self.to_sql(query)
        result = self.container.db.query(ordered_sql)
        rows = result.data
        num_rows = len(rows)

        def _accumulate_nested(
            rownum,
            nested_doc_details: DocumentDetails,
            parent_id: int,
            parent_id_coord: int,
        ) -> Tuple[int, List[Data]]:
            """
            :param rownum: index into rows for row
            :param nested_doc_details: {
                    "nested_path": wrap_nested_path(nested_path),
                    "index_to_column": map from column number to column details
                    "children": all possible direct decedents' nested_doc_details
                 }
            :param parent_id_coord: the id of the parent doc (for detecting when to step out of loop)
            :param id_index: the column number for this data (so we ca extract from each row)
            :return: (rownum, value) pair
            """
            output = []
            id_coord = nested_doc_details.id_coord
            curr_nested_path, _ = untype_field(nested_doc_details.nested_path[0])

            index_to_column = tuple(
                (i, c, c.push_list_name)
                for i, c in nested_doc_details.index_to_column.items()
            )
            row = rows[rownum]
            while True:
                doc = Null
                for i, c, rel_field in index_to_column:
                    value = c.pull(row)
                    if is_missing(value):
                        continue
                    doc = doc or Data()
                    try:
                        doc[rel_field] = value
                    except Exception as cause:
                        print(cause)
                rownum += 1

                for child_details in nested_doc_details.children:
                    # EACH NESTED TABLE MUST BE ASSEMBLED INTO A LIST OF OBJECTS
                    rownum, nested_value = _accumulate_nested(
                        rownum, child_details, row[id_coord], id_coord
                    )
                    if not nested_value:
                        continue
                    doc = doc or Data()
                    rel_field = relative_field(
                        untype_field(child_details.nested_path[0])[0], curr_nested_path
                    )
                    doc[rel_field] = unwraplist(nested_value)

                if doc or not parent_id:
                    output.append(doc)

                if rownum >= num_rows:
                    return rownum, output
                row = rows[rownum]
                if parent_id and parent_id != row[parent_id_coord]:
                    return rownum, output

        cols = tuple(i for i in index_to_column.values() if i.push_list_name != None)

        if rows:
            _, data = _accumulate_nested(0, primary_doc_details, 0, 0)
        else:
            data = result.data

        # the above returns data relative to snowflake.fact_name.  Get the nested_path
        rel_path = untype_field(relative_field(query.frum.nested_path[0], query.frum.schema.snowflake.fact_name))[0]
        if rel_path != '.':
            data = list_to_data(data).get(rel_path)

        if query.format == "cube":
            num_rows = len(data)
            header = tuple(jx.sort(set(c.push_column_name for c in cols)))
            if header == (".",):
                temp_data = {".": data}
            else:
                locs = tuple(literal_field(h) for h in header)
                temp_data = {h: [None] * num_rows for h in header}
                for rownum, d in enumerate(data):
                    for h, l in zip(header, locs):
                        temp_data[h][rownum] = d[l]
            return Data(
                meta={"format": "cube"},
                data=temp_data,
                edges=[{
                    "name": "rownum",
                    "domain": {
                        "type": "rownum",
                        "min": 0,
                        "max": num_rows,
                        "interval": 1,
                    },
                }],
            )
        elif query.format == "table":
            header = tuple(jx.sort(set(c.push_column_name for c in cols)))
            if header == (".",):
                temp_data = data
            else:
                locs = tuple(literal_field(h) for h in header)
                temp_data = [tuple(d[l] for l in locs) for d in data]

            return Data(meta={"format": "table"}, header=header, data=temp_data,)
        else:
            return Data(meta={"format": "list"}, data=data)

    def to_sql(self, query):
        # GET LIST OF SELECTED COLUMNS
<<<<<<< .mine
        select_vars = UNION([
            v for name, value, agg in query.select for v in value.vars()
        ])
||||||| .r1729
        select_vars = UNION([
            v for name, value in query.select for v in value.vars()
        ])
=======
        select_vars = set(
            rest if first == "row" else v
            for s in query.select.terms
            for v in s.value.vars()
            for first, rest in [tail_field(v)]
        )
>>>>>>> .r2071
        schema = query.frum.schema
        known_vars = schema.keys()
        active_paths = {schema.path: set()}
        for v in select_vars:
            for _, c in schema.leaves(v):
                active_paths.setdefault(c.nested_path[0], set()).add(c)

        # ANY VARS MENTIONED WITH NO COLUMNS?
        for v in select_vars:
            if not any(startswith_field(cname, v) for cname in known_vars):
                active_paths[schema.path].add(Column(
                    name=v,
                    jx_type=OBJECT,
                    es_column=".",
                    es_index=schema.path,
                    es_type="NULL",
                    nested_path=[schema.path],
                    multi=1,
                    cardinality=0,
                    last_updated=Date.now(),
                ))
        # EVERY COLUMN, AND THE COLUMN INDEX IT OCCUPIES
        index_to_column = {}  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
        index_to_uid = {}  # FROM ARRAY PATH TO THE INDEX OF UID
        sql_selects = []  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE)
        nest_to_alias = {
            nested_path: table_alias(i)
            for i, nested_path in enumerate(self.snowflake.query_paths)
        }
        # ADD SQL SELECT COLUMNS
        selects = query.select.partial_eval(SQLang)
        primary_doc_details = DocumentDetails("")
        # EVERY SELECT STATEMENT THAT WILL BE REQUIRED, NO MATTER THE DEPTH
        # WE WILL CREATE THEM ACCORDING TO THE DEPTH REQUIRED
        for sub_table in self.snowflake.tables:
            fact, step = tail_field(sub_table)
            nested_doc_details = DocumentDetails(sub_table)
            sub_schema = self.snowflake.get_schema(list(reversed([t for t in self.snowflake.tables if startswith_field(sub_table, t)])))

            if step == ".":
                # ROOT OF TREE
                primary_doc_details = nested_doc_details
            else:
                # INSERT INTO TREE
                def place(parent_doc_details: DocumentDetails):
                    if sub_table == parent_doc_details.nested_path[0]:
                        return True
                    if startswith_field(sub_table, parent_doc_details.nested_path[0]):
                        for c in parent_doc_details.children:
                            if place(c):
                                return True
                        parent_doc_details.children.append(nested_doc_details)
                        nested_doc_details.nested_path = [sub_table, *parent_doc_details.nested_path]

                place(primary_doc_details)

            nested_path = nested_doc_details.nested_path
            alias = nested_doc_details.alias = nest_to_alias[sub_table]

            # WE ALWAYS ADD THE UID
            column_number = index_to_uid[sub_table] = nested_doc_details.id_coord = len(sql_selects)
            sql_select = quote_column(alias, UID)
            sql_selects.append(sql_alias(sql_select, _make_column_name(column_number)))
            if step != ".":
                # ID FOR CHILD TABLE (REPLACE UID)
                index_to_column[column_number] = ColumnMapping(
                    sql=sql_select,
                    type="number",
                    nested_path=nested_path,
                    column_alias=_make_column_name(column_number),
                )

                # ORDER FOR CHILD TABLE
                column_number = len(sql_selects)
                sql_select = quote_column(alias, ORDER)
                sql_selects.append(sql_alias(
                    sql_select, _make_column_name(column_number)
                ))
                index_to_column[column_number] = ColumnMapping(
                    sql=sql_select,
                    type="number",
                    nested_path=nested_path,
                    column_alias=_make_column_name(column_number),
                )

            # WE DO NOT NEED DATA FROM TABLES WE REQUEST NOTHING FROM
            if sub_table not in active_paths:
                continue

<<<<<<< .mine
            for i, (name, value, agg) in enumerate(selects):
                if not any(value == c.es_column for c in active_paths[step]):
                    continue
||||||| .r1729
            for i, (name, value) in enumerate(selects):
=======
            sub_selects = selects.partial_eval(SQLang).to_sql(sub_schema).frum
            for i, term in enumerate(sub_selects.terms):
                name, value = term.name, term.value
>>>>>>> .r2071
                column_number = len(sql_selects)
                if is_op(value, LeavesOp):
                    Log.error("expecting SelectOp to subsume the LeavesOp")

                sql = value.partial_eval(SQLang).to_sql(sub_schema)
                column_alias = _make_column_name(column_number)
                sql_selects.append(sql_alias(sql, column_alias))
                push_column_name, push_column_child = tail_field(name)
                index_to_column[column_number] = nested_doc_details.index_to_column[column_number] = ColumnMapping(
                    push_list_name=name,
                    push_column_child=push_column_child,
                    push_column_name=unliteral_field(push_column_name),
                    push_column_index=i,
                    pull=get_column(column_number, json_type=value.jx_type),
                    sql=sql,
<<<<<<< .mine
                    type=json_type_to_simple_type(sql.type),
||||||| .r1729
                    type=jx_type_to_json_type(sql.type),
=======
                    type=jx_type_to_json_type(sql.jx_type),
>>>>>>> .r2071
                    column_alias=column_alias,
                    nested_path=nested_path,
                )
        where_clause = ToBooleanOp(query.where).partial_eval(SQLang).to_sql(schema)
        # ORDERING
        sorts = []
        if query.sort:
            for sort in query.sort:
                sql = sort.value.partial_eval(SQLang).to_sql(schema)
                column_number = len(sql_selects)
                # SQL HAS ABS TABLE REFERENCE
                column_alias = _make_column_name(column_number)
                sql_selects.append(sql_alias(sql, column_alias))
                sorts.append(quote_column(column_alias) + SQL_IS_NULL)
                sorts.append(quote_column(column_alias) + sort_to_sqlite_order[sort.sort])
        for t in self.snowflake.tables:
            sorts.append(quote_column(COLUMN + text(index_to_uid[t])))
        unsorted_sql = self._make_sql_for_one_nest_in_set_op(
            self.snowflake.fact_name,
            sql_selects,
            where_clause,
            active_paths,
            index_to_column,
            index_to_uid,
            query.limit,
            schema,
        )

        ordered_sql = [
            SQL_SELECT,
            SQL_STAR,
            SQL_FROM,
            sql_iso(unsorted_sql),
            SQL_ORDERBY,
            sql_list(sorts),
        ]
        ordered_sql = ConcatSQL(*ordered_sql)
        return index_to_column, ordered_sql, primary_doc_details

    def _make_sql_for_one_nest_in_set_op(
        self,
        primary_nested_path,
        selects,  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE
        where_clause,
        active_columns,
        index_to_sql_select,  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
        nested_path_to_uid_index,  # COLUMNS USED FOR UID (REQUIRED)
        limit,
        schema,
    ):
        """
        FOR EACH NESTED LEVEL, WE MAKE A QUERY THAT PULLS THE VALUES/COLUMNS REQUIRED
        WE `UNION ALL` THEM WHEN DONE
        """

        from_clause = ConcatSQL(
            SQL_FROM, sql_alias(quote_column(self.snowflake.fact_name), table_alias(0)),
        )
        with_clause = tuple()
        if limit is not NULL:
            with_clause = (
                SQL_WITH,
                quote_column(table_alias(0)),
                SQL_AS,
                sql_iso(
                    SQL_SELECT,
                    SQL_STAR,
                    from_clause,
                    SQL_LIMIT,
                    limit.to_sql(schema),
                )
            )
            from_clause = ConcatSQL(
                SQL_FROM,
                quote_column(table_alias(0)),
            )

        # STATEMENT FOR EACH NESTED PATH
        acc = []
        tables = self.snowflake.tables
<<<<<<< .mine
        for i, (nested_path, sub_table_name) in enumerate(tables):
            uid_indexes = set(
                i
                for n, i in nested_path_to_uid_index.items()
                if startswith_field(nested_path, n)
            )
||||||| .r1729
        for i, (nested_path, sub_table_name) in enumerate(tables):
            if any(startswith_field(nested_path, d) for d in done):
                continue
=======
        for i, sub_table_name in enumerate(tables):
            if any(startswith_field(sub_table_name, d) for d in done):
                continue
>>>>>>> .r2071

            select_clause = []
            # ADD SELECT CLAUSE HERE
            for select_index, select in enumerate(selects):
                column_mapping = index_to_sql_select.get(select_index)
                if not column_mapping:
                    select_clause.append(select)
                    continue

<<<<<<< .mine
                path = column_mapping.nested_path[0]
                if (
                    path == nested_path
                    or select_index in uid_indexes
                    or (
                        primary_nested_path != path
                        and startswith_field(primary_nested_path, path)
                    )
                ):
                    select_clause.append(sql_alias(
                        column_mapping.sql, column_mapping.column_alias
||||||| .r1729
            if primary_nested_path == nested_path:
                select_clause = []
                # ADD SELECT CLAUSE HERE
                for select_index, s in enumerate(selects):
                    column_mapping = index_to_sql_select.get(select_index)
                    if not column_mapping:
                        select_clause.append(s)
                        continue

                    if startswith_field(column_mapping.nested_path[0], nested_path):
                        select_clause.append(sql_alias(
                            column_mapping.sql, column_mapping.column_alias
                        ))
                    else:
                        # DO NOT INCLUDE DEEP STUFF AT THIS LEVEL
                        select_clause.append(sql_alias(
                            SQL_NULL, column_mapping.column_alias
                        ))

                if nested_path == ".":
                    from_clause.append(ConcatSQL(
                        SQL_FROM,
                        sql_alias(quote_column(self.snowflake.fact_name), alias),
=======
            if primary_nested_path == sub_table_name:
                select_clause = []
                # ADD SELECT CLAUSE HERE
                for select_index, s in enumerate(selects):
                    column_mapping = index_to_sql_select.get(select_index)
                    if not column_mapping:
                        select_clause.append(s)
                        continue

                    if startswith_field(column_mapping.nested_path[0], sub_table_name):
                        select_clause.append(sql_alias(
                            column_mapping.sql, column_mapping.column_alias
                        ))
                    else:
                        # DO NOT INCLUDE DEEP STUFF AT THIS LEVEL
                        select_clause.append(sql_alias(
                            SQL_NULL, column_mapping.column_alias
                        ))

                if sub_table_name == self.snowflake.fact_name:
                    from_clause.append(ConcatSQL(
                        SQL_FROM,
                        sql_alias(quote_column(self.snowflake.fact_name), alias),
>>>>>>> .r2071
                    ))
                else:
                    # DO NOT INCLUDE DEEP STUFF AT THIS LEVEL
                    select_clause.append(sql_alias(
                        SQL_NULL, column_mapping.column_alias
                    ))

            alias = table_alias(0)
            source_clauses = [from_clause]
            for ii, (np, stn) in enumerate(tables):
                parent_alias = alias
<<<<<<< .mine
                alias = table_alias(ii)
                if np != "." and startswith_field(nested_path, np):
                    source_clauses.append(ConcatSQL(
||||||| .r1729

            elif startswith_field(primary_nested_path, nested_path):
                # PARENT TABLE
                # NO NEED TO INCLUDE COLUMNS, BUT WILL INCLUDE ID AND ORDER
                if nested_path == ".":
                    from_clause.append(ConcatSQL(
                        SQL_FROM,
                        sql_alias(quote_column(self.snowflake.fact_name), alias),
                    ))
                else:
                    parent_alias = alias = table_alias(i)
                    from_clause.append(ConcatSQL(
=======

            elif startswith_field(primary_nested_path, sub_table_name):
                # PARENT TABLE
                # NO NEED TO INCLUDE COLUMNS, BUT WILL INCLUDE ID AND ORDER
                if sub_table_name == self.snowflake.fact_name:
                    from_clause.append(ConcatSQL(
                        SQL_FROM,
                        sql_alias(quote_column(self.snowflake.fact_name), alias),
                    ))
                else:
                    parent_alias = alias = table_alias(i)
                    from_clause.append(ConcatSQL(
>>>>>>> .r2071
                        SQL_LEFT_JOIN,
                        sql_alias(quote_column(stn), alias),
                        SQL_ON,
                        quote_column(alias, PARENT),
                        SQL_EQ,
                        quote_column(parent_alias, UID),
                    ))

<<<<<<< .mine
            acc.append(ConcatSQL(
                SQL_SELECT, sql_list(select_clause), ConcatSQL(*source_clauses)
            ))
||||||| .r1729
            elif startswith_field(nested_path, primary_nested_path):
                # CHILD TABLE
                # GET FIRST ROW FOR EACH NESTED TABLE
                from_clause.append(ConcatSQL(
                    SQL_LEFT_JOIN,
                    sql_alias(quote_column(sub_table_name), alias),
                    SQL_ON,
                    quote_column(alias, PARENT),
                    SQL_EQ,
                    quote_column(parent_alias, UID),
                    SQL_AND,
                    quote_column(alias, ORDER),
                    SQL_EQ,
                    SQL_ZERO,
                ))
=======
            elif startswith_field(sub_table_name, primary_nested_path):
                # CHILD TABLE
                # GET FIRST ROW FOR EACH NESTED TABLE
                from_clause.append(ConcatSQL(
                    SQL_LEFT_JOIN,
                    sql_alias(quote_column(sub_table_name), alias),
                    SQL_ON,
                    quote_column(alias, PARENT),
                    SQL_EQ,
                    quote_column(parent_alias, UID),
                    SQL_AND,
                    quote_column(alias, ORDER),
                    SQL_EQ,
                    SQL_ZERO,
                ))
>>>>>>> .r2071

<<<<<<< .mine
        sql = ConcatSQL(*with_clause, SQL_UNION_ALL.join(acc))
||||||| .r1729
                # IMMEDIATE CHILDREN ONLY
                done.append(nested_path)
                # NESTED TABLES WILL USE RECURSION
                children_sql.append(self._make_sql_for_one_nest_in_set_op(
                    nested_path,
                    selects,  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE
                    where_clause,
                    active_columns,
                    index_to_sql_select,  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
                    None,
                    None,
                    None
                ))
            else:
                # SIBLING PATHS ARE IGNORED
                continue

        sql = SQL_UNION_ALL.join(
            [ConcatSQL(
                SQL_SELECT,
                sql_list(select_clause),
                ConcatSQL(*from_clause),
                SQL_WHERE,
                where_clause,
            )]
            + children_sql
        )

=======
                # IMMEDIATE CHILDREN ONLY
                done.append(sub_table_name)
                # NESTED TABLES WILL USE RECURSION
                children_sql.append(self._make_sql_for_one_nest_in_set_op(
                    sub_table_name,
                    selects,  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE
                    where_clause,
                    active_columns,
                    index_to_sql_select,  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
                    None,
                    None,
                    None
                ))
            else:
                # SIBLING PATHS ARE IGNORED
                continue

        sql = SQL_UNION_ALL.join(
            [ConcatSQL(
                SQL_SELECT,
                sql_list(select_clause),
                ConcatSQL(*from_clause),
                SQL_WHERE,
                where_clause,
            )]
            + children_sql
        )

>>>>>>> .r2071
        return sql


def test_dots(cols):
    for c in cols:
        if "\\" in c.push_column_name:
            return True
    return False


class DocumentDetails(object):
    __slots__ = [
        "sub_table",
        "alias",
        "id_coord",
        "nested_path",
        "index_to_column",
        "children",
    ]

    def __init__(self, sub_table: str):
        self.sub_table: str = sub_table
        self.alias: str = ""
        self.id_coord: int = -1
        self.nested_path: List[str] = [sub_table]
        self.index_to_column: Dict[int, ColumnMapping] = {}
        self.children: List[DocumentDetails] = []
