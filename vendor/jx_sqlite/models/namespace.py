# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#


from copy import copy

from mo_sql.utils import SQL_ARRAY_KEY, untype_field

from mo_dots import startswith_field

from mo_future import first

import jx_base
from jx_base import Facts
from jx_sqlite.meta_columns import ColumnList
from jx_sqlite.models.schema import Schema
from jx_sqlite.models.snowflake import Snowflake


class Namespace(jx_base.Namespace):
    """
    MANAGE SQLITE DATABASE
    """

    def __init__(self, container):
        self.container = container
        self.columns = ColumnList(container.db)
        self.relations = self._load_relations()

    def __copy__(self):
        output = object.__new__(Namespace)
        output.db = None
        output.columns = copy(self.columns)
        return output

    def get_facts(self, fact_name):
        snowflake = Snowflake(fact_name, self)
        return Facts(self, snowflake)

    def get_schema(self, table_name):

        if SQL_ARRAY_KEY in table_name:
            cleaner = lambda x: x
        else:
            cleaner = lambda x: untype_field(x)[0]


        # TODO: HOW TO REDUCE RELATIONS TO JUST THIS TREE? (AVOID CYCLES)
        pair = first(
            (k, qps)
            for k, qps in self.columns._snowflakes.items()
            for qp in qps if cleaner(qp)==table_name
        )
        if pair is None:
            return None
        fact_name, query_paths = pair
        nested_path = []
        for query_path in query_paths:
            if startswith_field(table_name, query_path):
                nested_path.append(query_path)
        return Schema(list(reversed(nested_path)), Snowflake(nested_path[-1], self))

    def get_snowflake(self, fact_name):
        return Snowflake(fact_name, self)

    def get_relations(self):
        return self.relations[:]

    def get_columns(self, table_name):
        return self.columns.find_columns(table_name)

    def get_tables(self):
        return list(sorted(self.columns.data.keys()))

    def _load_relations(self):
        db = self.container.db
        return [r for t in db.get_tables() for r in db.get_relations(t.name)]

    def add_column_to_schema(self, column):
        self.columns.add(column)
