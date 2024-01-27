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

from jx_base.expressions import AbsOp as AbsOp_, TRUE
||||||| .r1729


from jx_base.expressions.is_number_op import IsNumberOp

from jx_base.expressions import AbsOp as AbsOp_
=======
from jx_base.expressions import AbsOp as _AbsOp, TRUE, IsNumberOp
>>>>>>> .r2071
from jx_sqlite.expressions._utils import SQLang, check
<<<<<<< .mine
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import SQL_NULL
from jx_sqlite.sqlite import sql_call
from mo_json import NUMBER, IS_NULL
||||||| .r1729
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import SQL_NULL, sql_call
from mo_json import JX_IS_NULL, JX_NUMBER
=======
from mo_imports import expect
from mo_json import JX_IS_NULL, JX_NUMBER
from mo_sqlite import SQL_NULL, sql_call
>>>>>>> .r2071

SqlScript = expect("SqlScript")


class AbsOp(_AbsOp):
    @check
    def to_sql(self, schema):
        expr = IsNumberOp(self.term).partial_eval(SQLang).to_sql(schema)
        if not expr:
<<<<<<< .mine
            return SQLScript(
                expr=SQL_NULL, data_type=T_IS_NULL, frum=self, miss=TRUE, schema=schema,
||||||| .r1729
            return SqlScript(
                expr=SQL_NULL, data_type=JX_IS_NULL, frum=self, schema=schema,
=======
            return SqlScript(
                expr=SQL_NULL, jx_type=JX_IS_NULL, frum=self, miss=TRUE, schema=schema,
>>>>>>> .r2071
            )

<<<<<<< .mine
        return SQLScript(
            expr=sql_call("ABS", expr), data_type=T_NUMBER, frum=self, schema=schema,
||||||| .r1729
        return SqlScript(
            expr=sql_call("ABS", expr), data_type=JX_NUMBER, frum=self, schema=schema,
=======
        return SqlScript(
            expr=sql_call("ABS", expr), jx_type=JX_NUMBER, frum=self, schema=schema,
>>>>>>> .r2071
        )
