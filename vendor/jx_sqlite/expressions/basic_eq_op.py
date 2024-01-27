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
    BasicEqOp as BasicEqOp_,
<<<<<<< .mine
    FALSE,
    is_literal,
    NotOp,
||||||| .r1729
    is_literal,
    NotOp,
=======
    FALSE,
    is_literal
>>>>>>> .r2071
    ToBooleanOp,
)
from jx_sqlite.expressions._utils import check, SQLang, value2boolean
from jx_sqlite.expressions.not_op import NotOp
<<<<<<< .mine
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import sql_iso, SQL_EQ
from mo_json.types import T_BOOLEAN
from mo_sql import ConcatSQL, SQL_NOT
||||||| .r1729
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import sql_iso, SQL_EQ
from mo_json.types import JX_BOOLEAN
from mo_sql import ConcatSQL
=======
from jx_sqlite.expressions.sql_script import SqlScript
from mo_json.types import JX_BOOLEAN
from mo_sql import ConcatSQL
>>>>>>> .r2071
from mo_sqlite import sql_iso, SQL_EQ


class BasicEqOp(BasicEqOp_):
    def partial_eval(self, lang):
        lhs = self.lhs.partial_eval(lang)
        rhs = self.rhs.partial_eval(lang)
        if is_literal(rhs) and rhs.value == 0:
<<<<<<< .mine
            lhs.data_type = T_BOOLEAN
||||||| .r1729
            lhs._data_type = JX_BOOLEAN
=======
            lhs._jx_type = JX_BOOLEAN
>>>>>>> .r2071
            return NotOp(lhs)
        if is_literal(lhs) and lhs.value == 0:
<<<<<<< .mine
            rhs.data_type = T_BOOLEAN
||||||| .r1729
            rhs._data_type = JX_BOOLEAN
=======
            rhs._jx_type = JX_BOOLEAN
>>>>>>> .r2071
            return NotOp(rhs)
        return BasicEqOp(lhs, rhs)

    @check
    def to_sql(self, schema):
        rhs = self.rhs.partial_eval(SQLang)
        lhs = self.lhs.partial_eval(SQLang)

        if is_literal(lhs):
            lhs, rhs = rhs, lhs
        if is_literal(rhs):
            lhs = lhs.to_sql(schema)
<<<<<<< .mine
            if lhs.data_type == T_BOOLEAN:
||||||| .r1729
            if lhs._data_type == JX_BOOLEAN:
=======
            if lhs.jx_type == JX_BOOLEAN:
>>>>>>> .r2071
                if value2boolean(rhs.value):
                    return lhs
                else:
                    return NotOp(lhs.frum).partial_eval(SQLang).to_sql(schema)
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
            expr=ConcatSQL(
                sql_iso(lhs.to_sql(schema)), SQL_EQ, sql_iso(rhs.to_sql(schema)),
            ),
            frum=self,
            miss=FALSE, schema=schema
        )
