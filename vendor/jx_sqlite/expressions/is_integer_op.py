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
from jx_base.expressions import IsIntegerOp as IsIntegerOp_, NULL
from jx_sqlite.expressions._utils import check
from mo_json.types import T_INTEGER


class IsIntegerOp(IsIntegerOp_):
    @check
    def to_sql(self, schema):
        value = self.term.to_sql(schema)
<<<<<<< .mine
        if value.data_type == T_INTEGER:
||||||| .r1729
        if value._data_type == JX_INTEGER:
=======
        if value.jx_type == JX_INTEGER:
>>>>>>> .r2071
            return value
        else:
            return NULL.to_sql(schema)
