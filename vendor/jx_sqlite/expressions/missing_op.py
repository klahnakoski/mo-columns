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

from jx_base.expressions import MissingOp as MissingOp_, FALSE
||||||| .r1729


from jx_base.expressions import MissingOp as MissingOp_
=======
from jx_base.expressions import MissingOp as MissingOp_, FALSE
>>>>>>> .r2071
from jx_base.language import is_op
from jx_sqlite.expressions._utils import SQLang, check
<<<<<<< .mine
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import ConcatSQL, SQL_IS_NULL, SQL_NOT, sql_call, SQL_OR, sql_iso, SQL_EQ, TextSQL, \
    SQL_EMPTY_STRING
from mo_json.types import T_BOOLEAN, T_TEXT
||||||| .r1729
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import ConcatSQL, SQL_IS_NULL, SQL_OR, sql_iso, SQL_EQ, SQL_EMPTY_STRING
from mo_json.types import JX_BOOLEAN, JX_TEXT
=======
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import ConcatSQL, SQL_IS_NULL, SQL_NOT, sql_call, SQL_OR, sql_iso, SQL_EQ, TextSQL, \
    SQL_EMPTY_STRING
from mo_json.types import JX_BOOLEAN, JX_TEXT
>>>>>>> .r2071


class MissingOp(MissingOp_):
    @check
    def to_sql(self, schema):
        sql = self.expr.partial_eval(SQLang).to_sql(schema)

        if is_op(sql.miss, MissingOp):
<<<<<<< .mine
            if sql.type == T_TEXT:
                return SQLScript(
                    data_type=T_BOOLEAN,
                    miss=FALSE,
||||||| .r1729
            if sql.type == JX_TEXT:
                return SqlScript(
                    data_type=JX_BOOLEAN,
=======
            if sql.jx_type == JX_TEXT:
                return SqlScript(
                    jx_type=JX_BOOLEAN,
                    miss=FALSE,
>>>>>>> .r2071
                    expr=sql_iso(
                        sql.expr,
                        SQL_IS_NULL,
                        SQL_OR,
                        sql_iso(sql.expr),
                        SQL_EQ,
                        SQL_EMPTY_STRING
                    ),
                    frum=self,
                    schema=schema
                )

<<<<<<< .mine
            return SQLScript(
                data_type=T_BOOLEAN,
                miss=FALSE,
                expr=ConcatSQL(sql.expr, SQL_IS_NULL),
||||||| .r1729
            return SqlScript(
                data_type=JX_BOOLEAN,
                expr=ConcatSQL(sql.sql_expr, SQL_IS_NULL),
=======
            return SqlScript(
                jx_type=JX_BOOLEAN,
                miss=FALSE,
                expr=ConcatSQL(sql.expr, SQL_IS_NULL),
>>>>>>> .r2071
                frum=self,
                schema=schema
            )

        expr = sql.miss.to_sql(schema)
<<<<<<< .mine
        return SQLScript(data_type=T_BOOLEAN, miss=FALSE, expr=expr, frum=sql.miss, schema=schema)
||||||| .r1729
        return SqlScript(data_type=JX_BOOLEAN, expr=expr, frum=sql.miss, schema=schema)
=======
        return SqlScript(jx_type=JX_BOOLEAN, miss=FALSE, expr=expr, frum=sql.miss, schema=schema)
>>>>>>> .r2071
