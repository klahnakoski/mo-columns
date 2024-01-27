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

from jx_base.expressions import MaxOp as MaxOp_, MissingOp
||||||| .r1729


from jx_base.expressions import MaxOp as MaxOp_, MissingOp
=======
from jx_base.expressions import MaxOp as _MaxOp, MissingOp
>>>>>>> .r2071
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import sql_call
from mo_json import T_NUMBER


class MaxOp(_MaxOp):
    @check
    def to_sql(self, schema):
        expr = sql_call("MAX", self.frum.to_sql(schema))
<<<<<<< .mine
        return SQLScript(
            data_type=T_NUMBER, miss=miss, expr=expr, frum=self, schema=schema
||||||| .r1729
        return SqlScript(
            data_type=JX_NUMBER, expr=expr, frum=self, schema=schema
=======
        return SqlScript(
            jx_type=JX_NUMBER, expr=expr, frum=self, schema=schema
>>>>>>> .r2071
        )
