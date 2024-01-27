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

from jx_base.expressions import AndOp as AndOp_
from jx_sqlite.expressions._utils import SQLang, check, SQLScript
from jx_sqlite.expressions.to_boolean_op import ToBooleanOp
||||||| .r1729


from jx_base.expressions import AndOp as AndOp_
from jx_sqlite.expressions._utils import SQLang, check, SqlScript
from jx_sqlite.expressions.to_boolean_op import ToBooleanOp
=======
from jx_base.expressions import AndOp as _AndOp, ToBooleanOp
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.sql_script import SqlScript
>>>>>>> .r2071
from jx_sqlite.sqlite import SQL_AND, SQL_FALSE, SQL_TRUE, sql_iso
from mo_json.types import T_BOOLEAN


class AndOp(_AndOp):
    @check
    def to_sql(self, schema):
        if not self.terms:
<<<<<<< .mine
            return SQLScript(data_type=T_BOOLEAN, expr=SQL_TRUE, frum=self)
||||||| .r1729
            return SqlScript(data_type=JX_BOOLEAN, expr=SQL_TRUE, frum=self)
=======
            return SqlScript(jx_type=JX_BOOLEAN, expr=SQL_TRUE, frum=self, schema=schema)
>>>>>>> .r2071
        elif all(self.terms):
<<<<<<< .mine
            return SQLScript(
                data_type=T_BOOLEAN,
||||||| .r1729
            return SqlScript(
                data_type=JX_BOOLEAN,
=======
            return SqlScript(
                jx_type=JX_BOOLEAN,
>>>>>>> .r2071
                expr=SQL_AND.join([
                    sql_iso(ToBooleanOp(t).partial_eval(SQLang).to_sql(schema))
                    for t in self.terms
                ]),
                frum=self,
                schema=schema
            )
        else:
<<<<<<< .mine
            return SQLScript(data_type=T_BOOLEAN, expr=SQL_FALSE, frum=self, schema=schema)
||||||| .r1729
            return SqlScript(data_type=JX_BOOLEAN, expr=SQL_FALSE, frum=self, schema=schema)
=======
            return SqlScript(jx_type=JX_BOOLEAN, expr=SQL_FALSE, frum=self, schema=schema)
>>>>>>> .r2071
