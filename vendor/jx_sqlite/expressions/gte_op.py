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

from jx_base.expressions import GteOp as GteOp_
||||||| .r1729


from jx_base.expressions import GteOp as GteOp_
=======
from jx_base.expressions import GteOp as _GteOp
>>>>>>> .r2071
from jx_sqlite.expressions._utils import _inequality_to_sql


class GteOp(_GteOp):
    to_sql = _inequality_to_sql
