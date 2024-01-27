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
from jx_base.expressions import (
    DivOp as _DivOp,
    MissingOp,
    OrOp,
    MissingOp,
    AndOp,
    OrOp,
    ToNumberOp,
)
<<<<<<< .mine
from jx_sqlite.expressions._utils import SQLang, check, SQLScript
from jx_sqlite.sqlite import sql_iso, ConcatSQL, sql_call, SQL_DIV
from mo_json import T_NUMBER
||||||| .r1729
from jx_sqlite.expressions._utils import SQLang, check, SqlScript
from mo_sqlite import sql_iso, ConcatSQL, sql_call, SQL_DIV
from mo_json import JX_NUMBER
=======
from jx_sqlite.expressions._utils import SQLang, check, SqlScript
from mo_json import JX_NUMBER
>>>>>>> .r2071
from mo_sqlite import sql_iso, ConcatSQL, SQL_DIV


class DivOp(_DivOp):
    @check
    def to_sql(self, schema):
        lhs = ToNumberOp(self.lhs).partial_eval(SQLang).to_sql(schema)
        rhs = ToNumberOp(self.rhs).partial_eval(SQLang).to_sql(schema)

<<<<<<< .mine
        if d.miss is TRUE:
            return SQLScript(
                data_type=T_NUMBER,
                expr=ConcatSQL(sql_iso(lhs), SQL_DIV, sql_iso(rhs)),
                frum=self,
                miss=OrOp([MissingOp(self.lhs), MissingOp(self.rhs)]),
                schema=schema,
            )
        else:
            return SQLScript(
                data_type=T_NUMBER | d.type,
                expr=sql_call(
                    "COALESCE", ConcatSQL(sql_iso(lhs), SQL_DIV, sql_iso(rhs)), d
                ),
                frum=self,
                miss=AndOp([
                    self.default.missing(SQLang),
                    OrOp([MissingOp(self.lhs), MissingOp(self.rhs)]),
                ]),
                schema=schema,
            )
||||||| .r1729
        if d.miss is TRUE:
            return SqlScript(
                data_type=JX_NUMBER,
                expr=ConcatSQL(sql_iso(lhs), SQL_DIV, sql_iso(rhs)),
                frum=self,
                schema=schema,
            )
        else:
            return SqlScript(
                data_type=JX_NUMBER | d.type,
                expr=sql_call(
                    "COALESCE", ConcatSQL(sql_iso(lhs), SQL_DIV, sql_iso(rhs)), d
                ),
                frum=self,
                schema=schema,
            )
=======
        return SqlScript(
            jx_type=JX_NUMBER,
            expr=ConcatSQL(sql_iso(lhs), SQL_DIV, sql_iso(rhs)),
            frum=self,
            miss=OrOp(MissingOp(self.lhs), MissingOp(self.rhs)),
            schema=schema,
        )
>>>>>>> .r2071
