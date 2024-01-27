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

from jx_base.expressions import SqlInstrOp as SqlInstrOp_, OrOp
from jx_sqlite.expressions._utils import check, SQLang, SQLScript
from jx_sqlite.sqlite import sql_call
from mo_json import T_INTEGER

||||||| .r1729


from jx_base.expressions import SqlInstrOp as SqlInstrOp_
from jx_sqlite.expressions._utils import check, SQLang, SqlScript
from mo_sqlite import sql_call
from mo_json import JX_INTEGER
=======
from jx_base.expressions import SqlInstrOp as SqlInstrOp_, OrOp
from jx_base.expressions._utils import simplified
from jx_sqlite.expressions._utils import check, SQLang, SqlScript
from mo_sqlite import sql_call
from mo_json import JX_INTEGER
>>>>>>> .r2071

class SqlInstrOp(SqlInstrOp_):
    @check
    def to_sql(self, schema):
        value = self.value.to_sql(schema)
        find = self.find.to_sql(schema)

<<<<<<< .mine
        return SQLScript(
            data_type=T_INTEGER,
            expr=sql_call("INSTR", value.expr, find.expr),
||||||| .r1729
        return SqlScript(
            data_type=JX_INTEGER,
            expr=sql_call("INSTR", value.frum, find.frum),
=======
        return SqlScript(
            jx_type=JX_INTEGER,
            expr=sql_call("INSTR", value.expr, find.expr),
>>>>>>> .r2071
            frum=self,
<<<<<<< .mine
            miss=OrOp([self.value.missing(SQLang), self.find.missing(SQLang)]),
||||||| .r1729
=======
            miss=OrOp(self.value.missing(SQLang), self.find.missing(SQLang)),
>>>>>>> .r2071
            schema=schema,
        )

    @simplified
    def partial_eval(self, lang):
        value = self.value.partial_eval(SQLang)
        find = self.find.partial_eval(SQLang)
        return SqlInstrOp(value, find)
