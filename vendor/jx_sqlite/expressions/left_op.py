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

from jx_base.expressions import LeftOp as LeftOp_, ONE, LengthOp, WhenOp, BasicSubstringOp, ZERO, MaxOp, MinOp, \
||||||| .r1729


from jx_base.expressions import LeftOp as LeftOp_, ONE, LengthOp, WhenOp, BasicSubstringOp, ZERO, MaxOp, MinOp, \
=======
from jx_base.expressions import LeftOp as _LeftOp, ONE, LengthOp, WhenOp, BasicSubstringOp, ZERO, MaxOp, MinOp, \
>>>>>>> .r2071
    SqlSubstrOp, EqOp
from jx_sqlite.expressions._utils import check, SQLang


class LeftOp(_LeftOp):
    @check
    def to_sql(self, schema):
        return (
            SqlSubstrOp(self.value, ONE, self.length)
            .partial_eval(SQLang)
            .to_sql(schema)
        )

    def partial_eval(self, lang):
        value = self.value.partial_eval(lang)
        length = self.length.partial_eval(lang)
        max_length = LengthOp(value)

        return WhenOp(
            EqOp(max_length, ZERO),
            **{"else": SqlSubstrOp(
                value,
                ONE,
                length,
            )}
        ).partial_eval(lang)
