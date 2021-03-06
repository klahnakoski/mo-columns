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

from jx_base.expressions import SelectOp as SelectOp_
from mo_logs.strings import quote


class SelectOp(SelectOp_):
    def to_python(self, not_null=False, boolean=False, many=False):
        return (
            "leaves_to_data({"
            + ",".join(
                quote(name + ":" + value.to_python()) for name, value in self
            )
            + "})"
        )
