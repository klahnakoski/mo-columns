# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import division
from __future__ import unicode_literals

import itertools
import os

from mo_testing.fuzzytestcase import assertAlmostEqual

import mo_json_config
from jx_base.expressions import QueryOp
from jx_python import jx
from jx_sqlite.container import Container
from jx_sqlite.query_table import QueryTable
from mo_dots import (
    wrap,
    coalesce,
    unwrap,
    listwrap,
    Data,
    startswith_field,
    to_data,
    is_many,
    is_sequence,
    Null,
)
from mo_files import File
from mo_future import text
from mo_json import json2value, types
from mo_kwargs import override
from mo_logs import Log, Except, constants
from mo_logs.exceptions import get_stacktrace
from tests import test_jx
from tests.test_jx import TEST_TABLE


class SQLiteUtils(object):
    @override
    def __init__(self, kwargs=None):
        self._index = None

    def setUp(self):
        container = Container(db=test_jx.global_settings.db)
        self._index = QueryTable(name="testing", container=container)

    def tearDown(self):

        pass

    def setUpClass(self):
        pass

    def tearDownClass(self):
        pass

    def not_real_service(self):
        return True

    def execute_tests(self, subtest, tjson=False, places=6):
        subtest = wrap(subtest)
        subtest.name = get_stacktrace()[1]["method"]

        if subtest.disable:
            return

        self.fill_container(subtest, typed=tjson)
        self.send_queries(subtest)

    def fill_container(self, subtest, typed=False):
        """
        RETURN SETTINGS THAT CAN BE USED TO POINT TO THE INDEX THAT'S FILLED
        """
        subtest = wrap(subtest)

        try:
            # INSERT DATA
            self._index.insert(subtest.data)
        except Exception as cause:
            Log.error(
                "can not load {{data}} into container", data=subtest.data, cause=cause
            )

        frum = subtest.query["from"]
        if isinstance(frum, text):
            subtest.query["from"] = frum.replace(TEST_TABLE, self._index.name)
        else:
            Log.error("Do not know how to handle")

        return to_data({"index": subtest.query["from"]})

    def send_queries(self, subtest):
        subtest = to_data(subtest)

        try:
            # EXECUTE QUERY
            num_expectations = 0
            for k, v in subtest.items():
                if k.startswith("expecting_"):  # WHAT FORMAT ARE WE REQUESTING
                    format = k[len("expecting_") :]
                elif k == "expecting":  # NO FORMAT REQUESTED (TO TEST DEFAULT FORMATS)
                    format = None
                else:
                    continue

                num_expectations += 1
                expected = v

                subtest.query.format = format
                subtest.query.meta.testing = True  # MARK ALL QUERIES FOR TESTING SO FULL METADATA IS AVAILABLE BEFORE QUERY EXECUTION
                try:
                    result = self.execute_query(subtest.query)
                except Exception as cause:
                    cause = Except.wrap(cause)
                    if format == "error":
                        if expected in cause:
                            return
                        else:
                            Log.error(
                                "Query failed, but for wrong reason; expected"
                                " {{expected}}, got {{reason}}",
                                expected=expected,
                                reason=cause,
                            )
                    else:
                        Log.error("did not expect error", cause=cause)

                compare_to_expected(subtest.query, result, expected)
            if num_expectations == 0:
                Log.error(
                    "Expecting test {{name|quote}} to have property named 'expecting_*'"
                    " for testing the various format clauses",
                    {"name": subtest.name},
                )
        except Exception as cause:
            Log.error("Failed test {{name|quote}}", name=subtest.name, cause=cause)

    def execute_update(self, command):
        return self._index.update(command)

    def execute_query(self, query):
        try:
            if startswith_field(query["from"], self._index.name):
                return self._index.query(query)
            elif query["from"] == "meta.columns":
                return self._index.query_metadata(query)
            else:
                Log.error("Do not know how to handle")
        except Exception as cause:
            Log.error("Failed query", cause)

    def try_till_response(self, *args, **kwargs):
        self.execute_query(json2value(kwargs["data"].decode("utf8")))


def compare_to_expected(query, result, expect):
    query = wrap(query)
    expect = wrap(expect)

    if result.meta.format == "table":
        assertAlmostEqual(set(result.header), set(expect.header))

        # MAP FROM expected COLUMN TO result COLUMN
        mapping = list(zip(
            *list(zip(
                *filter(
                    lambda v: v[0][1] == v[1][1],
                    itertools.product(
                        enumerate(expect.header), enumerate(result.header)
                    ),
                )
            ))[1]
        ))[0]
        result.header = [result.header[m] for m in mapping]

        if result.data:
            columns = list(zip(*unwrap(result.data)))
            result.data = zip(*[columns[m] for m in mapping])

        if not query.sort:
            sort_table(result)
            sort_table(expect)
    elif result.meta.format == "list":
        if query["from"].startswith("meta."):
            pass
        else:
            query = QueryOp.wrap(query, Null)

        if not query.sort:
            try:
                # result.data MAY BE A LIST OF VALUES, NOT OBJECTS
                data_columns = jx.sort(
                    set(jx.get_columns(result.data, leaves=True))
                    | set(jx.get_columns(expect.data, leaves=True)),
                    "name",
                )
            except Exception as _:
                data_columns = [{"name": "."}]

            sort_order = listwrap(coalesce(query.edges, query.groupby)) + data_columns

            if is_sequence(expect.data):
                try:
                    expect.data = jx.sort(expect.data, sort_order.name)
                except Exception:
                    pass

            if is_many(result.data):
                try:
                    result.data = jx.sort(result.data, sort_order.name)
                except Exception as cause:
                    Log.warning("sorting failed", cause=cause)

    elif (
        result.meta.format == "cube"
        and len(result.edges) == 1
        and result.edges[0].name == "rownum"
        and not query.sort
    ):
        result_data, result_header = cube2list(result.data)
        result_data = unwrap(jx.sort(result_data, result_header))
        result.data = list2cube(result_data, result_header)

        expect_data, expect_header = cube2list(expect.data)
        expect_data = jx.sort(expect_data, expect_header)
        expect.data = list2cube(expect_data, expect_header)

    # CONFIRM MATCH
    assertAlmostEqual(result, expect, places=6)


def cube2list(cube):
    """
    RETURNS header SO THAT THE ORIGINAL CUBE CAN BE RECREATED
    :param cube: A dict WITH VALUES BEING A MULTIDIMENSIONAL ARRAY OF UNIFORM VALUES
    :return: (rows, header) TUPLE
    """
    header = list(unwrap(cube).keys())
    rows = []
    for r in zip(*[[(k, v) for v in a] for k, a in cube.items()]):
        row = Data()
        for k, v in r:
            row[k] = v
        rows.append(unwrap(row))
    return rows, header


def list2cube(rows, header):
    output = {h: [] for h in header}
    for r in rows:
        for h in header:
            if h == ".":
                output[h].append(r)
            else:
                r = wrap(r)
                output[h].append(r[h])
    return output


def sort_table(result):
    """
    SORT ROWS IN TABLE, EVEN IF ELEMENTS ARE JSON
    """
    data = wrap([{text(i): v for i, v in enumerate(row)} for row in result.data])
    sort_columns = jx.sort(set(jx.get_columns(data, leaves=True).name))
    data = jx.sort(data, sort_columns)
    result.data = [
        tuple(row[text(i)] for i in range(len(result.header))) for row in data
    ]


def error(response):
    response = response.content.decode("utf8")

    try:
        e = Except.new_instance(json2value(response))
    except Exception:
        e = None

    if e:
        Log.error("Failed request", e)
    else:
        Log.error("Failed request\n {{response}}", {"response": response})


# read_alternate_settings
try:
    filename = os.environ.get("TEST_CONFIG")
    default_file = File("./tests/config/mo-columns.json")
    if filename:
        test_jx.global_settings = mo_json_config.get("file://" + filename)
        constants.set(test_jx.global_settings.constants)
    else:
        Log.alert(
            "No TEST_CONFIG environment variable to point to config file.  Using"
            f" {default_file.abspath}"
        )
        test_jx.global_settings = mo_json_config.get(f"file:///{default_file.abspath}")
        constants.set(test_jx.global_settings.constants)

    if not test_jx.global_settings.use:
        Log.error('Must have a {"use": type} set in the config file')

    Log.start(test_jx.global_settings.debug)
    test_jx.utils = SQLiteUtils(test_jx.global_settings)
except Exception as e:
    Log.warning("problem", e)


STRING_TYPED_COLUMN = types._S
