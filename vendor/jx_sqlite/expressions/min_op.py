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

from jx_base.expressions import MinOp as MinOp_
from jx_sqlite.expressions._utils import SQLang, check
||||||| .r1729


from jx_base.expressions import MinOp as MinOp_
from jx_sqlite.expressions._utils import SQLang, check
=======
from jx_base.expressions import MinOp as _MinOp
from jx_sqlite.expressions._utils import check
>>>>>>> .r2071
<<<<<<< .mine
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import sql_call
from mo_json import T_NUMBER
||||||| .r1729
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import sql_call
from mo_json import JX_NUMBER
=======
from jx_sqlite.expressions.sql_script import SqlScript
from mo_json import JX_NUMBER
from mo_sqlite import sql_call
>>>>>>> .r2071


class MinOp(_MinOp):
    @check
    def to_sql(self, schema):
<<<<<<< .mine
        terms = [t.partial_eval(SQLang).to_sql(schema).expr for t in self.terms]
        return SQLScript(
            data_type=T_NUMBER, expr=sql_call("MIN", *terms), frum=self, schema=schema
||||||| .r1729
        terms = [t.partial_eval(SQLang).to_sql(schema).frum for t in self.terms]
        return SqlScript(
            data_type=JX_NUMBER, expr=sql_call("MIN", *terms), frum=self, schema=schema
=======
        return SqlScript(
            jx_type=JX_NUMBER, expr=sql_call("MIN", self.frum.to_sql(schema)), frum=self, schema=schema
>>>>>>> .r2071
        )
