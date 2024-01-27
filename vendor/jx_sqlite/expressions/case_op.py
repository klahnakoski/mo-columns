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

from jx_base.expressions import CaseOp as CaseOp_
||||||| .r1729


from jx_base.expressions import CaseOp as CaseOp_
=======
from jx_base.expressions import CaseOp as _CaseOp
>>>>>>> .r2071
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import (
    SQL_CASE,
    SQL_ELSE,
    SQL_END,
    SQL_THEN,
    SQL_WHEN,
    ConcatSQL,
)
from mo_json import union_type


class CaseOp(_CaseOp):
    @check
    def to_sql(self, schema):
        if len(self.whens) == 1:
            return self.whens[-1].partial_eval(SQLang).to_sql(schema)

        acc = [SQL_CASE]
        data_type = []
        for w in self.whens[:-1]:
            when = w.when.partial_eval(SQLang).to_sql(schema)
            value = w.then.partial_eval(SQLang).to_sql(schema)
<<<<<<< .mine
            data_type.append(value.type)
||||||| .r1729
            _data_type.append(value.type)
=======
            data_type.append(value.jx_type)
>>>>>>> .r2071
            acc.append(ConcatSQL(SQL_WHEN, when, SQL_THEN, value))

        value = self.whens[-1].partial_eval(SQLang).to_sql(schema)
<<<<<<< .mine
        data_type.append(value.type)
||||||| .r1729
        _data_type.append(value.type)
=======
        data_type.append(value.jx_type)
>>>>>>> .r2071
        acc.append(ConcatSQL(SQL_ELSE, value, SQL_END,))

<<<<<<< .mine
        return SQLScript(
            data_type=union_type(*data_type),
||||||| .r1729
        return SqlScript(
            data_type=union_type(*_data_type),
=======
        return SqlScript(
            jx_type=union_type(*data_type),
>>>>>>> .r2071
            expr=ConcatSQL(*acc),
            frum=self,
            miss=self.missing(SQLang),
            schema=schema,
        )
