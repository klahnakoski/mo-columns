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

from jx_base.expressions import ToBooleanOp as ToBooleanOp_, FALSE, TRUE, is_literal
||||||| .r1729


from jx_base.expressions import ToBooleanOp as ToBooleanOp_, FALSE, TRUE, is_literal
=======
from jx_base.expressions import ToBooleanOp as ToBooleanOp_
>>>>>>> .r2071
from jx_sqlite.expressions._utils import SQLang, check
<<<<<<< .mine
from mo_dots import Null
from mo_json.types import T_BOOLEAN, base_type
||||||| .r1729
from mo_dots import Null
from mo_json.types import JX_BOOLEAN, base_type
=======
from mo_json import JX_BOOLEAN
>>>>>>> .r2071


class ToBooleanOp(ToBooleanOp_):
    @check
    def to_sql(self, schema):
        term = self.term.partial_eval(SQLang)
<<<<<<< .mine
        if base_type(term.type) == T_BOOLEAN:
||||||| .r1729
        if base_type(term.type) == JX_BOOLEAN:
=======
        if term.jx_type == JX_BOOLEAN:
>>>>>>> .r2071
            return term.to_sql(schema)
        else:
            return term.exists().to_sql(schema)