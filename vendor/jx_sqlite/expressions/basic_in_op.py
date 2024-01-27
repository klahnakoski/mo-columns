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
    BasicInOp as BasicInOp_,
    FALSE,
    Literal, Variable, ExistsOp, NestedOp, EqOp,
)
from jx_base.expressions.variable import is_variable
from jx_base.language import is_op
from jx_sqlite.expressions._utils import check, SQLang, value2boolean
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import quote_list
from mo_json.types import T_BOOLEAN
from mo_logs import Log
from mo_sql import ConcatSQL, SQL_IN


class BasicInOp(BasicInOp_):
    @check
    def to_sql(self, schema):
        value = self.value.partial_eval(SQLang).to_sql(schema)
        superset = self.superset.partial_eval(SQLang)
        if is_op(superset, Literal):
            values = superset.value
<<<<<<< .mine
            if value.data_type == T_BOOLEAN:
||||||| .r1729
            if value._data_type == JX_BOOLEAN:
=======
            if value.jx_type == JX_BOOLEAN:
>>>>>>> .r2071
                values = [value2boolean(v) for v in values]
            # TODO: DUE TO LIMITED BOOLEANS, TURN THIS INTO EqOp
            sql = ConcatSQL(value, SQL_IN, quote_list(values))
<<<<<<< .mine
            return SQLScript(
                data_type=T_BOOLEAN, expr=sql, frum=self, miss=FALSE, schema=schema
||||||| .r1729
            return SqlScript(
                data_type=JX_BOOLEAN, expr=sql, frum=self, schema=schema
=======
            return SqlScript(
                jx_type=JX_BOOLEAN, expr=sql, frum=self, miss=FALSE, schema=schema
>>>>>>> .r2071
            )

        if not is_variable(superset):
            Log.error("Do not know how to hanldle")

        sub_table = schema.get_table(superset.var)
        return ExistsOp(NestedOp(
            nested_path=sub_table.nested_path,
            where=EqOp(Variable("."), value.frum)
        )).to_sql(schema)
