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
from jx_base.expressions import ToIntegerOp as IntegerOp_
from jx_sqlite.expressions._utils import check, SQLScript
from jx_sqlite.sqlite import sql_cast
from mo_json import base_type, T_TEXT, T_INTEGER


class ToIntegerOp(IntegerOp_):
    @check
    def to_sql(self, schema):
        value = self.term.to_sql(schema)

<<<<<<< .mine
        if base_type(value) == T_TEXT:
            return SQLScript(
                data_type=T_INTEGER,
||||||| .r1729
        if base_type(value) == JX_TEXT:
            return SqlScript(
                data_type=JX_INTEGER,
=======
        if base_type(value) == JX_TEXT:
            return SqlScript(
                jx_type=JX_INTEGER,
>>>>>>> .r2071
                expr=sql_cast(value, "INTEGER"),
                frum=self,
                miss=value.miss,
                schea=schema,
            )
        return value
