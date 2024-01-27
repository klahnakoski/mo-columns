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

from jx_base.expressions import BasicIndexOfOp as BasicIndexOfOp_, FALSE
from jx_sqlite.expressions._utils import check, SQLScript
||||||| .r1729


from jx_base.expressions import BasicIndexOfOp as BasicIndexOfOp_
from jx_sqlite.expressions._utils import check, SqlScript
=======
from jx_base.expressions import BasicIndexOfOp as BasicIndexOfOp_, FALSE
from jx_sqlite.expressions._utils import check, SqlScript
>>>>>>> .r2071
from jx_sqlite.expressions.literal import Literal
from jx_sqlite.sqlite import sql_call
from mo_json.types import T_NUMBER
from mo_sql import (
    SQL_CASE,
    SQL_ELSE,
    SQL_END,
    SQL_THEN,
    SQL_WHEN,
    SQL_ONE,
    ConcatSQL,
    SQL_NEG,
)


class BasicIndexOfOp(BasicIndexOfOp_):
<<<<<<< .mine
    data_type = T_NUMBER
||||||| .r1729
    _data_type = JX_NUMBER
=======
    data_type = JX_NUMBER
>>>>>>> .r2071

    @check
    def to_sql(self, schema):
        value = self.value.to_sql(schema)
        find = self.find.to_sql(schema)
        start = self.start

        if isinstance(start, Literal) and start.value == 0:
            expr = ConcatSQL(sql_call("INSTR", value, find), SQL_NEG, SQL_ONE)

<<<<<<< .mine
            return SQLScript(expr=expr, miss=FALSE, frum=self)
||||||| .r1729
            return SqlScript(expr=expr, frum=self)
=======
            return SqlScript(expr=expr, miss=FALSE, frum=self)
>>>>>>> .r2071
        else:
            start_index = start.to_sql(schema)
            found = sql_call(
                "INSTR", sql_call("SUBSTR", value, start_index), SQL_ONE, find
            )
            return SQLScript(ConcatSQL(
                SQL_CASE,
                SQL_WHEN,
                found,
                SQL_THEN,
                found,
                "+",
                start_index,
                "-1",
                SQL_ELSE,
                "-1",
                SQL_END,
            ))
