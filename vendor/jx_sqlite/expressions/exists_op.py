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
from jx_base.expressions import ExistsOp as ExistsOp_, FALSE
from jx_sqlite.expressions._utils import check, SQLang, SQLScript
from mo_json import T_BOOLEAN


class ExistsOp(ExistsOp_):
    @check
    def to_sql(self, schema):
        sql = self.expr.partial_eval(SQLang).to_sql(schema)
<<<<<<< .mine
        return SQLScript(
            data_type=T_BOOLEAN, expr=sql, frum=self, miss=FALSE, schema=schema
||||||| .r1729
        return SqlScript(
            data_type=JX_BOOLEAN, expr=sql, frum=self, schema=schema
=======
        return SqlScript(
            jx_type=JX_BOOLEAN, expr=sql, frum=self, miss=FALSE, schema=schema
>>>>>>> .r2071
        )
