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
    NotRightOp as NotRightOp_,
    LengthOp,
    MaxOp,
    SubOp,
    Literal,
    ZERO,
)
<<<<<<< .mine
from jx_sqlite.expressions._utils import check, OrOp, SQLang
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import SQL_ONE
from jx_sqlite.sqlite import sql_call
from mo_json import T_TEXT
||||||| .r1729
from jx_sqlite.expressions._utils import check, SQLang
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import SQL_ONE
from mo_sqlite import sql_call
from mo_json import JX_TEXT
=======
from jx_sqlite.expressions._utils import check, OrOp, SQLang
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import SQL_ONE
from mo_sqlite import sql_call
from mo_json import JX_TEXT
>>>>>>> .r2071


class NotRightOp(NotRightOp_):
    @check
    def to_sql(self, schema):
        v = self.value.to_sql(schema)
        if self.length == ZERO:
            return v

        r = self.length.to_sql(schema)
        end = (
            MaxOp(ZERO, SubOp(LengthOp(self.value), MaxOp(ZERO, self.length)))
            .partial_eval(SQLang)
            .to_sql(schema)
        )
<<<<<<< .mine
        sql = sql_call("SUBSTR", v.expr, SQL_ONE, end)
        return SQLScript(
            data_type=T_TEXT,
||||||| .r1729
        sql = sql_call("SUBSTR", v.frum, SQL_ONE, end)
        return SqlScript(
            data_type=JX_TEXT,
=======
        sql = sql_call("SUBSTR", v.expr, SQL_ONE, end)
        return SqlScript(
            jx_type=JX_TEXT,
>>>>>>> .r2071
            expr=sql,
            frum=self,
<<<<<<< .mine
            miss=OrOp([r.miss, v.miss]),
||||||| .r1729
=======
            miss=OrOp(r.miss, v.miss),
>>>>>>> .r2071
            schema=schema,
        )
