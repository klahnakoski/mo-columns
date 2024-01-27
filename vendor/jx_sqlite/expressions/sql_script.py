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
<<<<<<< .mine
    FALSE,
    SQLScript as SQLScript_,
    TRUE,
    Variable,
||||||| .r1729
    SqlScript as _SQLScript,
    Expression,
=======
    FALSE,
    SqlScript as _SQLScript,
    TRUE,
    MissingOp,
>>>>>>> .r2071
)
<<<<<<< .mine
from jx_base.language import is_op
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.sqlite import (
    SQL,
    SQL_CASE,
    SQL_END,
    SQL_NULL,
    SQL_THEN,
    SQL_WHEN,
    sql_iso,
    ConcatSQL,
    SQL_NOT,
)
from mo_future import PY2, text
||||||| .r1729
from jx_base.models.schema import Schema
from jx_sqlite.expressions._utils import check
from mo_sqlite import SQL
=======
from jx_base.expressions.variable import is_variable
from jx_base.language import is_op
from jx_sqlite.expressions._utils import SQLang, check
>>>>>>> .r2071
from mo_imports import export
from mo_json import JsonType
from mo_sqlite import *


<<<<<<< .mine
class SQLScript(SQLScript_, SQL):
    __slots__ = ("data_type", "expr", "frum", "miss", "schema")
||||||| .r1729
class SqlScript(_SQLScript, SQL):
    __slots__ = ("_data_type", "sql_expr", "frum", "schema")
=======
class SqlScript(_SQLScript, SQL):
    __slots__ = ("_jx_type", "expr", "frum", "miss", "schema")
>>>>>>> .r2071

<<<<<<< .mine
    def __init__(self, data_type, expr, frum, miss=None, schema=None):
||||||| .r1729
    def __init__(
        self,
        data_type: JxType,
        expr: SQL,
        frum: Expression,
        schema: Schema
    ):
=======
    def __init__(self, jx_type, expr, frum, miss=None, schema=None):
>>>>>>> .r2071
        object.__init__(self)
        if expr == None:
            Log.error("expecting expr")
        if not isinstance(expr, SQL):
            Log.error("Expecting SQL")
<<<<<<< .mine
        if not isinstance(data_type, JsonType):
            Log.error("Expecting JsonType")
||||||| .r1729
        if not isinstance(data_type, JxType):
            Log.error("Expecting JxType")
=======
        if not isinstance(jx_type, JxType):
            Log.error("Expecting JsonType")
>>>>>>> .r2071
        if schema is None:
            Log.error("expecting schema")

<<<<<<< .mine
        if miss is None:
            self.miss = frum.missing(SQLang)
        else:
            self.miss = miss
        self.data_type = data_type  # JSON DATA TYPE
        self.expr = expr
||||||| .r1729
        self._data_type = data_type  # JSON DATA TYPE
        self.sql_expr = expr
=======
        if miss is None:
            self.miss = frum.missing(SQLang)
        else:
            self.miss = miss
        self._jx_type = jx_type
        self.expr = expr
>>>>>>> .r2071
        self.frum = frum  # THE ORIGINAL EXPRESSION THAT MADE expr
        self.schema = schema

    @property
<<<<<<< .mine
    def type(self):
        return self.data_type

    @property
||||||| .r1729
    def type(self) -> JxType:
        return self._data_type

    @property
=======
>>>>>>> .r2071
    def name(self):
        return "."

    def __getitem__(self, item):
        if not self.many:
            if item == 0:
                return self
            else:
                Log.error("this is a primitive value")
        else:
            Log.error("do not know how to handle")

    def __iter__(self):
        """
        ASSUMED TO OVERRIDE SQL.__iter__()
        """
<<<<<<< .mine
        return self.sql.__iter__()
||||||| .r1729
        return self.sql_expr.__iter__()
=======
        return self._sql().__iter__()
>>>>>>> .r2071

    def to_sql(self, schema):
        return self

    @property
    def sql(self):
<<<<<<< .mine
        self.miss = self.miss.partial_eval(SQLang)
        if self.miss is TRUE:
            return SQL_NULL
        elif self.miss is FALSE or is_op(self.frum, Variable):
            return self.expr
||||||| .r1729
        return self.sql_expr
=======
        return self._sql()
>>>>>>> .r2071

<<<<<<< .mine
        missing = self.miss.partial_eval(SQLang).to_sql(self.schema)
        return ConcatSQL(
            SQL_CASE, SQL_WHEN, SQL_NOT, sql_iso(missing), SQL_THEN, self.expr, SQL_END,
        )

||||||| .r1729
=======
    def _sql(self):
        self.miss = self.miss.partial_eval(SQLang)
        if self.miss is TRUE:
            return SQL_NULL
        elif self.miss is FALSE or is_variable(self.frum):
            return self.expr

        if is_op(self.miss, MissingOp) and is_variable(self.frum) and self.miss.expr == self.frum:
            return self.expr

        missing = self.miss.to_sql(self.schema)
        if str(missing) == str(SQL_ZERO):
            self.miss = FALSE
            return self.expr
        if str(missing) == str(SQL_ONE):
            self.miss = TRUE
            return SQL_NULL

        return ConcatSQL(
            SQL_CASE, SQL_WHEN, SQL_NOT, sql_iso(missing), SQL_THEN, self.expr, SQL_END,
        )

>>>>>>> .r2071
    def __str__(self):
        return str(self._sql())

<<<<<<< .mine
    def __unicode__(self):
        return text(self.sql)

    def __add__(self, other):
        return text(self) + text(other)

    def __radd__(self, other):
        return text(other) + text(self)

    if PY2:
        __unicode__ = __str__

||||||| .r1729
=======
    def __add__(self, other):
        return text(self) + text(other)

    def __radd__(self, other):
        return text(other) + text(self)

>>>>>>> .r2071
    @check
    def to_sql(self, schema):
        return self

    def missing(self, lang):
        return self.miss

    def __data__(self):
        return {"script": self.script}

    def __eq__(self, other):
        if not isinstance(other, SQLScript_):
            return False
<<<<<<< .mine
        elif self.expr == other.expr:
            return True
        else:
            return False
||||||| .r1729
        return self.sql_expr == other.sql_expr
=======
        return self.expr == other.expr
>>>>>>> .r2071


<<<<<<< .mine
export("jx_sqlite.expressions._utils", SQLScript)
export("jx_sqlite.expressions.or_op", SQLScript)
||||||| .r1729
export("jx_sqlite.expressions._utils", SqlScript)
export("jx_sqlite.expressions.or_op", SqlScript)
=======
export("jx_sqlite.expressions._utils", SqlScript)
export("jx_sqlite.expressions.or_op", SqlScript)
export("jx_sqlite.expressions.abs_op", SqlScript)
>>>>>>> .r2071
