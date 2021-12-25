# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from datetime import datetime, date
from decimal import Decimal

from mo_dots import NullType
from mo_future import text, none_type
from mo_json.types import _B, _I, _N, _T, _S, _J
from mo_times import Date

python_type_to_json_type_key = {
    bool: _B,
    int: _I,
    float: _N,
    Decimal: _N,
    Date: _T,
    datetime: _T,
    date: _T,
    text: _S,
    NullType: _J,
    none_type: _J,
}

json_type_key_to_sql_type = {
    _B: "TINYINT",
    _I: "INTEGER",
    _N: "REAL",
    _S: "TEXT",
    _T: "REAL"
}
