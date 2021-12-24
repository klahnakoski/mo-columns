# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import (
    FALSE,
    FalseOp,
    NULL,
    NullOp,
    TrueOp,
    extend,
    TRUE,
)
from jx_base.expressions._utils import TYPE_CHECK
from jx_base.language import Language
from jx_sqlite.sqlite import (
    SQL,
    SQL_FALSE,
    SQL_NULL,
    SQL_TRUE,
    sql_iso,
    SQL_ZERO,
    SQL_ONE,
    SQL_PLUS,
    SQL_STAR,
    SQL_LT,
    ConcatSQL,
    SQL_AS,
    SQL_SELECT,
    SQL_FROM,
    SQL_WITH,
    SQL_DIV,
    SQL_GT,
    SQL_LE,
    SQL_GE,
)
from jx_sqlite.sqlite import sql_call
from mo_future import decorate
from mo_imports import expect
from mo_json import BOOLEAN, ARRAY, OBJECT, STRING, NUMBER, IS_NULL, TIME, INTERVAL
from mo_json.types import T_IS_NULL, T_BOOLEAN, T_NUMBER, T_TIME, T_INTERVAL, T_TEXT
from mo_logs import Log

ToNumberOp, OrOp, SQLScript = expect("ToNumberOp", "OrOp", "SQLScript")


def check(func):
    """
    TEMPORARY TYPE CHECKING TO ENSURE to_sql() IS OUTPUTTING THE CORRECT FORMAT
    """
    if not TYPE_CHECK:
        return func

    @decorate(func)
    def to_sql(self, schema):
        try:
            output = func(self, schema)
        except Exception as e:
            # output = func(self, schema)
            raise Log.error("not expected", cause=e)
        if not isinstance(output, SQLScript):
            output = func(self, schema)
            Log.error("expecting SQLScript")
        return output

    return to_sql


@extend(NullOp)
@check
def to_sql(self, schema):
    return SQLScript(
        data_type=T_IS_NULL, expr=SQL_NULL, frum=self, miss=TRUE, schema=schema
    )


@extend(TrueOp)
@check
def to_sql(self, schema):
    return SQLScript(
        data_type=T_BOOLEAN, expr=SQL_TRUE, frum=self, miss=FALSE, schema=schema
    )


@extend(FalseOp)
@check
def to_sql(self, schema):
    return SQLScript(
        data_type=T_BOOLEAN, expr=SQL_FALSE, frum=self, miss=FALSE, schema=schema
    )


def _inequality_to_sql(self, schema):
    op, identity = _sql_operators[self.op]

    lhs = ToNumberOp(self.lhs).partial_eval(SQLang).to_sql(schema)
    rhs = ToNumberOp(self.rhs).partial_eval(SQLang).to_sql(schema)

    sql = sql_call(
        "COALESCE", ConcatSQL(sql_iso(lhs.expr), op, sql_iso(rhs.expr)), SQL_ZERO
    )

    return SQLScript(
        data_type=T_BOOLEAN, expr=sql, frum=self, miss=FALSE, schema=schema
    )


@check
def _binaryop_to_sql(self, schema):
    op, identity = _sql_operators[self.op]

    lhs = ToNumberOp(self.lhs).partial_eval(SQLang).to_sql(schema)
    rhs = ToNumberOp(self.rhs).partial_eval(SQLang).to_sql(schema)

    sql = ConcatSQL(sql_iso(lhs.expr), op, sql_iso(rhs.expr))
    missing = OrOp([self.lhs.missing(SQLang), self.rhs.missing(SQLang),])

    return SQLScript(
        data_type=T_NUMBER, expr=sql, frum=self, miss=missing, schema=schema,
    )


def multiop_to_sql(self, schema):
    sign, zero = _sql_operators[self.op]
    if len(self.terms) == 0:
        return self.default.partial_eval(SQLang).to_sql(schema)
    elif self.default is NULL:
        return sign.join(
            sql_call("COALESCE", t.partial_eval(SQLang).to_sql(schema), zero)
            for t in self.terms
        )
    else:
        return sql_call(
            "COALESCE",
            sign.join(
                sql_iso(t.partial_eval(SQLang).to_sql(schema)) for t in self.terms
            ),
            self.default.partial_eval(SQLang).to_sql(schema),
        )


def with_var(var, expression, eval):
    """
    :param var: NAME (AS SQL) GIVEN TO expression
    :param expression: THE EXPRESSION TO COMPUTE FIRST
    :param eval: THE EXPRESSION TO COMPUTE SECOND, WITH var ASSIGNED
    :return: PYTHON EXPRESSION
    """
    x = SQL("x")

    return sql_iso(
        SQL_WITH,
        x,
        SQL_AS,
        sql_iso(SQL_SELECT, sql_iso(expression), SQL_AS, var),
        SQL_SELECT,
        eval,
        SQL_FROM,
        x,
    )


def basic_multiop_to_sql(self, schema, many=False):
    op, identity = _sql_operators[self.op.split("basic.")[1]]
    sql = op.join(sql_iso(t.partial_eval(SQLang).to_sql(schema)) for t in self.terms)
    return SQLScript(
        data_type=T_NUMBER,
        frum=self,
        expr=sql,
        miss=FALSE,  # basic operations are "strict"
        schema=schema,
    )


SQLang = Language("SQLang")


_sql_operators = {
    # (operator, zero-array default value) PAIR
    "add": (SQL_PLUS, SQL_ZERO),
    "sum": (SQL_PLUS, SQL_ZERO),
    "mul": (SQL_STAR, SQL_ONE),
    "sub": (SQL(" - "), None),
    "div": (SQL_DIV, None),
    "exp": (SQL(" ** "), None),
    "mod": (SQL(" % "), None),
    "gt": (SQL_GT, None),
    "gte": (SQL_GE, None),
    "lte": (SQL_LE, None),
    "lt": (SQL_LT, None),
}

SQL_IS_NULL_KEY = "0"
SQL_BOOLEAN_KEY = "b"
SQL_NUMBER_KEY = "n"
SQL_TIME_KEY = "t"
SQL_INTERVAL_KEY = "n"
SQL_STRING_KEY = "s"
SQL_OBJECT_KEY = "j"
SQL_ARRAY_KEY = "a"

json_type_to_sql_key = {
    IS_NULL: SQL_IS_NULL_KEY,
    BOOLEAN: SQL_BOOLEAN_KEY,
    NUMBER: SQL_NUMBER_KEY,
    TIME: SQL_TIME_KEY,
    INTERVAL: SQL_INTERVAL_KEY,
    STRING: SQL_STRING_KEY,
    OBJECT: SQL_OBJECT_KEY,
    ARRAY: SQL_ARRAY_KEY,
    T_IS_NULL: SQL_IS_NULL_KEY,
    T_BOOLEAN: SQL_BOOLEAN_KEY,
    T_NUMBER: SQL_NUMBER_KEY,
    T_TIME: SQL_TIME_KEY,
    T_INTERVAL: SQL_INTERVAL_KEY,
    T_TEXT: SQL_STRING_KEY,
}

sql_type_to_json_type = {
    None: None,
    "0": IS_NULL,
    "b": BOOLEAN,
    "n": NUMBER,
    "s": STRING,
    "j": OBJECT,
}
