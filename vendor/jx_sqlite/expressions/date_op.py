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

from jx_base.expressions import DateOp as DateOp_
||||||| .r1729


from jx_base.expressions import DateOp as DateOp_
=======
from jx_base.expressions import DateOp as _DateOp
>>>>>>> .r2071
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import quote_value


class DateOp(_DateOp):
    @check
    def to_sql(self, schema):
        value = self.value
<<<<<<< .mine
        return SQLScript(data_type=self.data_type, expr=quote_value(value), frum=self, schema=schema)
||||||| .r1729
        return SqlScript(data_type=self._data_type, expr=quote_value(value), frum=self, schema=schema)
=======
        return SqlScript(jx_type=self.data_type, expr=quote_value(value), frum=self, schema=schema)
>>>>>>> .r2071
