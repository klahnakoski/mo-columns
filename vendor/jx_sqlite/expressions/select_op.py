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

from jx_base.expressions import SelectOp as SelectOp_, LeavesOp, Variable, AndOp, NULL
from jx_base.language import is_op
from jx_sqlite.expressions._utils import check, SQLang
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import (
||||||| .r1729


from dataclasses import dataclass
from typing import List

from jx_base.expressions import SelectOp as SelectOp_, LeavesOp, Variable, NULL
from jx_base.expressions.variable import get_variable
from jx_base.language import is_op
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import (
=======
from jx_base.expressions import SelectOp as SelectOp_, LeavesOp, Variable, AndOp, NULL
from jx_base.expressions.select_op import SelectOne
from jx_base.expressions.variable import is_variable
from jx_base.language import is_op, is_expression
from jx_sqlite.expressions._utils import check, SQLang
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import (
>>>>>>> .r2071
    quote_column,
    SQL_COMMA,
    SQL_AS,
    SQL_SELECT,
    SQL,
    Log,
    ENABLE_TYPE_CHECKING,
    SQL_CR,
)
from mo_dots import concat_field, literal_field
<<<<<<< .mine
from mo_json.types import to_json_type, T_IS_NULL
||||||| .r1729
from mo_json.types import to_jx_type, JX_IS_NULL
from mo_sql import ConcatSQL, SQL_FROM, sql_iso
=======
from mo_json.types import JX_IS_NULL, to_jx_type
>>>>>>> .r2071


class SelectOp(SelectOp_):
    @check
    def to_sql(self, schema):
<<<<<<< .mine
        type = T_IS_NULL
||||||| .r1729
        frum_sql = self.frum.to_sql(schema)
        schema = frum_sql.schema

        type = JX_IS_NULL
=======
        jx_type = JX_IS_NULL
>>>>>>> .r2071
        sql_terms = []
        diff = False
<<<<<<< .mine
        for name, expr, agg in self:
            if is_op(expr, Variable):
||||||| .r1729
        for name, expr in self:
            expr = get_variable(expr)

            if is_op(expr, Variable):
=======
        for term in self.terms:
            name, expr, agg, default = term.name, term.value, term.aggregate, term.default
            if is_variable(expr):
>>>>>>> .r2071
                var_name = expr.var
                cols = list(schema.leaves(var_name))
                if len(cols) == 0:
<<<<<<< .mine
                    sql_terms.append({
                        "name": name,
                        "value": NULL,
                        "aggregate": agg,
                    })
||||||| .r1729
                    sql_terms.append(SelectOneSQL(name, NULL.to_sql(schema)))
=======
                    sql_terms.append(SelectOne(name, NULL, agg, default))
>>>>>>> .r2071
                    continue
<<<<<<< .mine
                elif len(cols) == 1:
                    rel_name0, col0 = cols[0]
                    if col0.es_column == var_name:
                        # WHEN WE REQUEST AN ES_COLUMN DIRECTLY, BREAK THE RECURSIVE LOOP
                        full_name = concat_field(name, rel_name0)
                        type |= full_name + to_json_type(col0.jx_type)
                        sql_terms.append({
                            "name": full_name,
                            "value": expr,
                            "aggregate": agg,
                        })
                        continue

                diff = True
                for rel_name, col in cols:
                    full_name = concat_field(name, rel_name)
                    type |= full_name + to_json_type(col.jx_type)
                    sql_terms.append({
                        "name": full_name,
                        "value": Variable(col.es_column, col.jx_type),
                        "aggregate": agg,
                    })
||||||| .r1729
                else:
                    for rel_name, col in cols:
                        full_name = concat_field(name, rel_name)
                        type |= full_name + to_jx_type(col.json_type)
                        sql_terms.append(SelectOneSQL(full_name, Variable(col.es_column, col.json_type).to_sql(schema)))
=======
                elif len(cols) == 1:
                    rel_name0, col0 = cols[0]
                    if col0.es_column == var_name:
                        # WHEN WE REQUEST AN ES_COLUMN DIRECTLY, BREAK THE RECURSIVE LOOP
                        full_name = concat_field(name, rel_name0)
                        jx_type |= full_name + to_jx_type(col0.json_type)
                        sql_terms.append(SelectOne(full_name, expr, agg, default))
                        continue

                diff = True
                for rel_name, col in cols:
                    full_name = concat_field(name, rel_name)
                    jx_type |= full_name + to_jx_type(col.json_type)
                    sql_terms.append(SelectOne(full_name, Variable(col.es_column, col.json_type), agg, default))
>>>>>>> .r2071
            elif is_op(expr, LeavesOp):
                var_names = expr.vars()
                for var_name in var_names:
                    cols = schema.leaves(var_name)
                    diff = True
                    for rel_name, col in cols:
<<<<<<< .mine
                        if name == '.':
                            full_name = literal_field(rel_name)
                        else:
                            full_name = name + "\\." + literal_field(rel_name)
                        type |= full_name + to_json_type(col.jx_type)
                        sql_terms.append({
                            "name": full_name,
                            "value": Variable(col.es_column, col.jx_type),
                            "aggregate": agg
                        })
||||||| .r1729
                        full_name = concat_field(name,  literal_field(rel_name))
                        type |= full_name + to_jx_type(col.json_type)
                        sql_terms.append(SelectOneSQL(full_name, Variable(col.es_column, col.json_type).to_sql(schema)))
=======
                        full_name = concat_field(name, literal_field(rel_name))
                        jx_type |= full_name + to_jx_type(col.json_type)
                        sql_terms.append(SelectOne(full_name, Variable(col.es_column, col.json_type), agg, default))
>>>>>>> .r2071
            else:
<<<<<<< .mine
                type |= name + to_json_type(expr.type)
                sql_terms.append({
                    "name": name,
                    "value": expr,
                    "aggregate": agg,
                })
||||||| .r1729
                type |= name + to_jx_type(expr.type)
                sql_terms.append(SelectOneSQL(name, expr.to_sql(schema)))
=======
                jx_type |= name + to_jx_type(expr.jx_type)
                sql_terms.append(SelectOne(name, expr, agg, default))
>>>>>>> .r2071

<<<<<<< .mine
        if diff:
            return SelectOp(sql_terms).partial_eval(SQLang).to_sql(schema)

        return SQLScript(
||||||| .r1729
        return SqlScript(
=======
        if diff:
            return SelectOp(self.frum, *sql_terms).partial_eval(SQLang).to_sql(schema)

        return SqlScript(
>>>>>>> .r2071
<<<<<<< .mine
            data_type=type,
            expr=SelectSQL(sql_terms, schema),
            miss=AndOp([t["value"].missing(SQLang) for t in sql_terms]),
||||||| .r1729
            data_type=type,
            expr=ConcatSQL(SelectSQL(sql_terms), SQL_FROM, sql_iso(frum_sql)),
=======
            jx_type=jx_type,
            expr=SelectSQL([{"name":t.name, "value":t.expr} for t in sql_terms], schema),
            miss=AndOp(*(t.expr.missing(SQLang) for t in sql_terms)),  # TODO: should be False?
>>>>>>> .r2071
            frum=self,
            schema=schema,
        )


class SelectSQL(SQL):
    __slots__ = ["terms", "schema"]

    def __init__(self, terms, schema):
        if ENABLE_TYPE_CHECKING:
<<<<<<< .mine
            if not isinstance(terms, list) or not all(isinstance(term, dict) for term in terms):
                Log.error("expecting list of dicts")
||||||| .r1729
            if not isinstance(terms, list) or any(not isinstance(term, SelectOneSQL) for term in terms):
                Log.error("expecting list of SelectOne")
=======
            if not isinstance(terms, list) or not all(isinstance(term, dict) for term in terms):
                Log.error("expecting list of dicts")
            if not all(is_expression(term["value"]) for term in terms):
                Log.error("expecting list of dicts with expressions")
>>>>>>> .r2071
        self.terms = terms
        self.schema = schema

    def __iter__(self):
        yield from SQL_SELECT
        comma = SQL_CR
        for term in self.terms:
<<<<<<< .mine
            name, value = term["name"], term["value"]
            for s in comma:
                yield s
||||||| .r1729
            name, value = term.name, term.value
            yield from comma
=======
            name, value = term["name"], term["value"]
            yield from comma
>>>>>>> .r2071
            comma = SQL_COMMA
<<<<<<< .mine
            for s in value.to_sql(self.schema):
                yield s
            for s in SQL_AS:
                yield s
            for s in quote_column(name):
                yield s
||||||| .r1729
            yield from value
            yield from SQL_AS
            yield from quote_column(name)
=======
            yield from value.to_sql(self.schema)
            yield from SQL_AS
            yield from quote_column(name)
>>>>>>> .r2071
