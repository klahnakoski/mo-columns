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
from jx_base.expressions import RegExpOp as RegExpOp_
<<<<<<< .mine
from jx_sqlite.expressions._utils import check, SQLang, SQLScript, OrOp
from jx_sqlite.sqlite import TextSQL, ConcatSQL
from mo_json import T_BOOLEAN
||||||| .r1729
from jx_sqlite.expressions._utils import check, SQLang, SqlScript
from mo_sqlite import TextSQL, ConcatSQL
from mo_json import JX_BOOLEAN
=======
from jx_sqlite.expressions._utils import check, SQLang, SqlScript, OrOp
from mo_sqlite import TextSQL, ConcatSQL
from mo_json import JX_BOOLEAN
>>>>>>> .r2071


class RegExpOp(RegExpOp_):
    @check
    def to_sql(self, schema):
        pattern = self.pattern.partial_eval(SQLang).to_sql(schema)
        expr = self.expr.partial_eval(SQLang).to_sql(schema)
<<<<<<< .mine
        return SQLScript(
            data_type=T_BOOLEAN,
            expr=ConcatSQL(expr.expr, TextSQL(" REGEXP "), pattern.expr),
||||||| .r1729
        return SqlScript(
            data_type=JX_BOOLEAN,
            expr=ConcatSQL(expr.frum, TextSQL(" REGEXP "), pattern.frum),
=======
        return SqlScript(
            jx_type=JX_BOOLEAN,
            expr=ConcatSQL(expr.expr, TextSQL(" REGEXP "), pattern.expr),
>>>>>>> .r2071
            frum=self,
<<<<<<< .mine
            miss=OrOp([expr.missing(SQLang), pattern.missing(SQLang)]),
||||||| .r1729
=======
            miss=OrOp(expr.missing(SQLang), pattern.missing(SQLang)),
>>>>>>> .r2071
            schema=schema,
        )
