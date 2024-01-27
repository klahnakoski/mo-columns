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
from jx_base.expressions import (
    NotLeftOp as NotLeftOp_,
    GteOp,
    LengthOp,
    AddOp,
    MaxOp,
    ZERO,
    ONE,
)
<<<<<<< .mine
from jx_sqlite.expressions._utils import check, SQLang, SQLScript, OrOp
from jx_sqlite.sqlite import sql_call, SQL_ZERO, ConcatSQL, SQL_ONE, SQL_PLUS
from mo_json import T_TEXT
||||||| .r1729
from jx_sqlite.expressions._utils import check, SQLang, SqlScript
from mo_sqlite import sql_call
from mo_json import JX_TEXT
=======
from jx_sqlite.expressions._utils import check, SQLang, SqlScript, OrOp
from mo_sqlite import sql_call, SQL_ZERO, ConcatSQL, SQL_ONE, SQL_PLUS
from mo_json import JX_TEXT
>>>>>>> .r2071


class NotLeftOp(NotLeftOp_):
    @check
    def to_sql(self, schema):
        v = self.value.to_sql(schema)
        start = (
            AddOp(MaxOp(ZERO, self.length), ONE, nulls=False).partial_eval(SQLang).to_sql(schema)
        )

        expr = sql_call("SUBSTR", v, start)
<<<<<<< .mine
        return SQLScript(
            data_type=T_TEXT,
||||||| .r1729
        return SqlScript(
            data_type=JX_TEXT,
=======
        return SqlScript(
            jx_type=JX_TEXT,
>>>>>>> .r2071
            expr=expr,
            frum=self,
<<<<<<< .mine
            miss=OrOp([
                self.value.missing(SQLang),
                self.length.missing(SQLang),
                GteOp([self.length, LengthOp(self.value)]),
            ]),
||||||| .r1729
=======
            miss=OrOp(
                self.value.missing(SQLang),
                self.length.missing(SQLang),
                GteOp(self.length, LengthOp(self.value)),
            ),
>>>>>>> .r2071
            schema=schema,
        )
