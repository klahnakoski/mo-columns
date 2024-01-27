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
from jx_base.expressions import NULL, SqlSubstrOp as SqlSubstrOp_
from jx_sqlite.expressions._utils import check, SQLang, OrOp
from jx_sqlite.expressions.literal import Literal
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import sql_call
from mo_json import T_TEXT


class SqlSubstrOp(SqlSubstrOp_):
    @check
    def to_sql(self, schema):
        value = self.value.partial_eval(SQLang).to_sql(schema)
        start = self.start.partial_eval(SQLang).to_sql(schema)
        if self.length is NULL:
            sql = sql_call("SUBSTR", value, start)
        else:
            length = self.length.partial_eval(SQLang).to_sql(schema)
            sql = sql_call("SUBSTR", value, start, length)
<<<<<<< .mine
        return SQLScript(
            data_type=T_TEXT,
||||||| .r1729
        return SqlScript(
            data_type=JX_TEXT,
=======
        return SqlScript(
            jx_type=JX_TEXT,
>>>>>>> .r2071
            expr=sql,
            frum=self,
<<<<<<< .mine
            miss=OrOp([value.miss, start.miss]),
||||||| .r1729
=======
            miss=OrOp(value.miss, start.miss),
>>>>>>> .r2071
            schema=schema,
        )

    def partial_eval(self, lang):
        value = self.value.partial_eval(SQLang)
        start = self.start.partial_eval(SQLang)
        length = self.length.partial_eval(SQLang)
        if isinstance(start, Literal) and start.value == 1:
            if length is NULL:
                return value
        return SqlSubstrOp(value, start, length)
