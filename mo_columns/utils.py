# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_math import randoms
from mo_sql.utils import json_type_to_sql_type_key

from mo_json.types import BOOLEAN_KEY, INTEGER_KEY, NUMBER_KEY, TIME_KEY, STRING_KEY, python_type_to_jx_type, \
    jx_type_to_json_type

json_type_key_to_sqlite_type = {
    BOOLEAN_KEY: "TINYINT",
    INTEGER_KEY: "INTEGER",
    NUMBER_KEY: "REAL",
    STRING_KEY: "TEXT",
    TIME_KEY: "REAL",
}


def python_type_to_sql_type_key(python_type):
    jx_type = python_type_to_jx_type(python_type)
    json_type = jx_type_to_json_type(jx_type)
    return json_type_to_sql_type_key[json_type]


def uuid():
    return randoms.base64(20)

