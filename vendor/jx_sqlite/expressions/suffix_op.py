# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import SuffixOp as SuffixOp_, FALSE, TRUE
from jx_sqlite.expressions._utils import check, SQLang
from jx_sqlite.expressions.eq_op import EqOp
from jx_sqlite.expressions.length_op import LengthOp
from jx_sqlite.expressions.literal import Literal
from jx_sqlite.expressions.right_op import RightOp


class SuffixOp(SuffixOp_):
    @check
    def to_sql(self, schema):
        if not self.expr:
            return FALSE.to_sql(schema)
        elif isinstance(self.suffix, Literal) and not self.suffix.value:
            return TRUE.to_sql(schema)
        else:
            return EqOp(RightOp(self.expr, LengthOp(self.suffix)), self.suffix).partial_eval(SQLang).to_sql(schema)
