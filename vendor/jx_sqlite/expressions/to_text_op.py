# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import ToTextOp as ToTextOp_, SelectOp, CoalesceOp
from jx_base.language import is_op
from jx_sqlite.expressions._utils import check, SQLang
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import (
    SQL_CASE,
    SQL_ELSE,
    SQL_END,
    SQL_THEN,
    SQL_WHEN,
    sql_iso,
    ConcatSQL, sql_cast,
)
from jx_sqlite.sqlite import quote_value, sql_call
from mo_json import T_TEXT, T_BOOLEAN, T_NUMBER_TYPES, split_field, base_type


class ToTextOp(ToTextOp_):
    @check
    def to_sql(self, schema):
        expr = self.term.to_sql(schema)
        type = base_type(expr.type)
        if type == T_TEXT:
            return expr
        elif type == T_BOOLEAN:
            return SQLScript(
                data_type=T_TEXT,
                expr=ConcatSQL(
                    SQL_CASE,
                    SQL_WHEN,
                    sql_iso(expr.expr),
                    SQL_THEN,
                    quote_value("true"),
                    SQL_ELSE,
                    quote_value("false"),
                    SQL_END,
                ),
                frum=self,
                schema=schema,
            )
        elif type in T_NUMBER_TYPES:
            return SQLScript(
                data_type=T_TEXT,
                expr=sql_call(
                    "RTRIM",
                    sql_call(
                        "RTRIM",
                        sql_cast(expr.expr, "TEXT"),
                        quote_value("0"),
                    ),
                    quote_value("."),
                ),
                frum=self,
                schema=schema,
            )
        elif is_op(expr.frum, SelectOp) and len(expr.frum.terms) > 1:
            return CoalesceOp([
                ToTextOp(t['value'])
                for t in expr.frum.terms
                if len(split_field(t['name'])) == 1
            ]).partial_eval(SQLang).to_sql(schema)
        else:
            return SQLScript(
                data_type=T_TEXT,
                expr=sql_cast(expr.expr, "TEXT"),
                frum=self,
                schema=schema,
            )
