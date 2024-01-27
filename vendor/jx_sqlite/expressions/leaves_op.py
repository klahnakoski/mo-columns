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

from jx_base.expressions import LeavesOp as LeavesOp_, NULL
from jx_base.expressions.select_op import SelectOp
from jx_base.language import is_op
||||||| .r1729


from jx_base.expressions import LeavesOp as LeavesOp_, NULL
from jx_base.expressions.select_op import SelectOp
from jx_base.language import is_op
=======
from jx_base.expressions import LeavesOp as LeavesOp_, CoalesceOp
from jx_base.expressions.select_op import SelectOp, SelectOne
from jx_base.expressions.variable import is_variable
>>>>>>> .r2071
from jx_sqlite.expressions._utils import check, SQLang
from jx_sqlite.expressions.variable import Variable
from mo_dots import Null, concat_field, literal_field
from mo_logs import Log


class LeavesOp(LeavesOp_):
    @check
    def to_sql(self, schema):
        if not is_variable(self.term):
            Log.error("Can only handle Variable")
        var_name = self.term.var
        leaves = list(schema.leaves(var_name))
        unique = set(r for r, _ in leaves)

<<<<<<< .mine
        flat = SelectOp([
            {
                "name": literal_field(r),
                "value": Variable(c.es_column),
                "aggregate": NULL
            }
            for r, c in schema.leaves(self.term.var)
        ])
||||||| .r1729
        flat = SelectOp(schema, *[
            {
                "name": literal_field(r),
                "value": Variable(c.es_column),
                "aggregate": NULL
            }
            for r, c in schema.leaves(self.term.var)
        ])
=======
        flat = SelectOp(
            Null,
            *(
                SelectOne(
                    literal_field(r),
                    CoalesceOp(*(Variable(c.es_column) for rr, c in leaves if rr == r)).partial_eval(SQLang),
                )
                for r in unique
            )
        )
        if len(flat.terms) == 1:
            return flat.terms[0].value.to_sql(schema)
>>>>>>> .r2071

        return flat.partial_eval(SQLang).to_sql(schema)
