# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_math import randoms

from mo_json.types import _B, _I, _N, _T, _S
from mo_sql import SQL

json_type_key_to_sqlite_type = {
    _B: SQL("TINYINT"),
    _I: SQL("INTEGER"),
    _N: SQL("REAL"),
    _S: SQL("TEXT"),
    _T: SQL("REAL")
}


def uuid():
    return randoms.base64(20)

