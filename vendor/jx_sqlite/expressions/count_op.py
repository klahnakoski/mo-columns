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

from jx_base.expressions import CountOp as CountOp_, FALSE
from jx_sqlite.expressions._utils import SQLang, check
||||||| .r1729


from jx_base.expressions import CountOp as CountOp_
from jx_sqlite.expressions._utils import SQLang, check
=======
from jx_base.expressions import CountOp as _CountOp
from jx_sqlite.expressions._utils import check
>>>>>>> .r2071
<<<<<<< .mine
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import JoinSQL, SQL_SUM, sql_iso
from mo_json import T_INTEGER
||||||| .r1729
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import JoinSQL, SQL_SUM, sql_iso
from mo_json import JX_INTEGER
=======
from jx_sqlite.expressions.sql_script import SqlScript
from mo_json import JX_INTEGER
>>>>>>> .r2071
from mo_sqlite import sql_call


class CountOp(_CountOp):
    @check
    def to_sql(self, schema):
<<<<<<< .mine
        acc = []
        for term in self.terms:
            m = term.missing(SQLang).invert(SQLang).partial_eval(SQLang)
            acc.append(sql_iso(m.to_sql(schema).expr))
        return SQLScript(
            data_type=T_INTEGER,
            expr=JoinSQL(SQL_SUM, acc),
||||||| .r1729
        acc = []
        for term in self.terms:
            m = term.missing(SQLang).invert(SQLang).partial_eval(SQLang)
            acc.append(sql_iso(m.to_sql(schema).frum))
        return SqlScript(
            data_type=JX_INTEGER,
            expr=JoinSQL(SQL_SUM, acc),
=======
        return SqlScript(
            jx_type=JX_INTEGER,
            expr=sql_call("MAX", self.frum.to_sql(schema).expr),
>>>>>>> .r2071
            frum=self,
            miss=FALSE,
            schema=schema,
        )
