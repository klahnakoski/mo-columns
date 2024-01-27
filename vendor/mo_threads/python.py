# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

import os

from mo_dots import set_default, to_data, from_data
from json import dumps as value2json, loads as json2value
from mo_future import is_windows
from mo_logs import Except, logger
from mo_threads import Lock, Process, Signal, THREAD_STOP, Thread, DONE, python_worker

PYTHON = "python"
DEBUG = True


class Python(object):

    def __init__(self, name, config):
        config = to_data(config)
        if config.debug.logs:
            logger.error("not allowed to configure logging on other process")

        logger.info("begin process in dir={dir}", dir=os.getcwd())
        # WINDOWS REQUIRED shell, WHILE LINUX NOT
        shell = is_windows
        python_worker_file = os.path.abspath(python_worker.__file__)
        self.process = Process(
<<<<<<< .mine
            name,
            [PYTHON, "-u", "mo_threads" + os.sep + "python_worker.py"],
            debug=False,
            cwd=os.getcwd(),
            shell=shell
||||||| .r1729
            name, [python_exe, "-u", f"mo_threads{os.sep}python_worker.py"], debug=DEBUG, cwd=os.getcwd(), shell=shell,
=======
            name,
            [python_exe, "-u", python_worker_file],
            env={**os.environ, "PYTHONPATH": "."},
            debug=DEBUG,
            cwd=os.getcwd(),
            shell=shell,
>>>>>>> .r2071
        )
<<<<<<< .mine
        self.process.stdin.add(value2json(from_data(config | {"debug": {"trace": True}})))
        status = self.process.stdout.pop()
        if status != '{"out":"ok"}':
            Log.error("could not start python\n{{error|indent}}", error=self.process.stderr.pop_all()+[status]+self.process.stdin.pop_all())
        self.lock = Lock("wait for response from "+name)
        self.current_task = DONE
        self.current_response = None
        self.current_error = None
||||||| .r1729
        self.process.stdin.add(value2json(from_data(
            config
            | {
                "debug": {"trace": True},
                "constants": {"mo_threads": {"signals": {"DEBUG": False}, "lock": {"DEBUG": False},}},
            }
        )))
        while True:
            line = self.process.stdout.pop()
            if line == '{"out":"ok"}':
                break
            Log.note("waiting to start python: {{line}}", line=line)
        self.lock = Lock("wait for response from " + name)
        self.stop_error = None
        self.done = DONE
        self.response = None
        self.error = None
=======
        self.process.stdin.add(value2json(from_data(
            config
            | {
                "debug": {"trace": True},
                "constants": {"mo_threads": {"signals": {"DEBUG": False}, "lock": {"DEBUG": False}}},
            }
        )))
        while True:
            line = self.process.stdout.pop()
            if line == THREAD_STOP:
                logger.error("problem starting python process: STOP detected on stdout")
            if line == '{"out":"ok"}':
                break
            logger.info("waiting to start python: {line}", line=line)
        self.lock = Lock("wait for response from " + name)
        self.stop_error = None
        self.done = DONE
        self.response = None
        self.error = None
>>>>>>> .r2071

        self.daemon = Thread.run("", self._daemon)
        self.errors = Thread.run("", self._stderr)

    def _execute(self, command):
        with self.lock:
            self.current_task.wait()
            self.current_task = Signal()
            self.current_response = None
            self.current_error = None

<<<<<<< .mine
            if self.process.service_stopped:
                Log.error("python is not running")
            self.process.stdin.add(value2json(command))
            (self.current_task | self.process.service_stopped).wait()
||||||| .r1729
        self.response = None
        self.error = None
        self.process.stdin.add(value2json(command), force=True)
        self.done.wait()
        try:
            if self.error:
                Log.error("problem with process call", cause=Except(**self.error))
            else:
                return self.response
        finally:
            self.response = None
            self.error = None
=======
        self.response = None
        self.error = None
        self.process.stdin.add(value2json(command), force=True)
        self.done.wait()
        try:
            if self.error:
                logger.error("problem with process call", cause=Except(**self.error))
            else:
                return self.response
        finally:
            self.response = None
            self.error = None
>>>>>>> .r2071

            try:
                if self.current_error:
                    Log.error("problem with process call", cause=Except(**self.current_error))
                else:
                    return self.current_response
            finally:
                self.current_task = DONE
                self.current_response = None
                self.current_error = None

    def _daemon(self, please_stop):
        while not please_stop:
            line = self.process.stdout.pop(till=please_stop)
<<<<<<< .mine
||||||| .r1729
            DEBUG and Log.note("stdout got {{line}}", line=line)
=======
            DEBUG and logger.info("stdout got {line}", line=line)
>>>>>>> .r2071
            if line == THREAD_STOP:
                break

            try:
                data = to_data(json2value(line))
            except Exception:
                logger.info("non-json line: {line}", line=line)
                continue

            try:
                if "log" in data:
                    logger.main_log.write(**data.log)
                elif "out" in data:
                    self.current_response = data.out
                    self.current_task.go()
                elif "err" in data:
                    self.current_error = data.err
                    self.current_task.go()
            except Exception as cause:
                logger.error("unexpected problem", cause=cause)
        DEBUG and logger.info("stdout reader is done")

    def _stderr(self, please_stop):
        while not please_stop:
            try:
                line = self.process.stderr.pop(till=please_stop)
<<<<<<< .mine
                if line == THREAD_STOP:
||||||| .r1729
                if line is None or line == THREAD_STOP:
=======
                DEBUG and logger.info("stderr got {line}", line=line)
                if line is None or line == THREAD_STOP:
>>>>>>> .r2071
                    please_stop.go()
                    break
<<<<<<< .mine
                Log.note("Error line from {{name}}({{pid}}): {{line}}", line=line, name=self.process.name, pid=self.process.pid)
||||||| .r1729
                Log.note(
                    "Error line from {{name}}({{pid}}): {{line}}",
                    line=line,
                    name=self.process.name,
                    pid=self.process.pid,
                )
=======
                logger.info(
                    "Error line from {name}({pid}): {line}", line=line, name=self.process.name, pid=self.process.pid,
                )
>>>>>>> .r2071
            except Exception as cause:
                logger.error("could not process line", cause=cause)
        DEBUG and logger.info("stderr reader is done")

    def import_module(self, module_name, var_names=None):
        if var_names is None:
            self._execute({"import": module_name})
        else:
            self._execute({"import": {"from": module_name, "vars": var_names}})

    def set(self, var_name, value):
        self._execute({"set": {var_name, value}})

    def get(self, var_name):
        return self._execute({"get": var_name})

    def execute_script(self, script):
        return self._execute({"exec": script})

    def __getattr__(self, item):
        def output(*args, **kwargs):
            if len(args):
                if kwargs.keys():
                    logger.error("Not allowed to use both args and kwargs")
                return self._execute({item: args})
            else:
                return self._execute({item: kwargs})
        return output

    def stop(self):
<<<<<<< .mine
        self._execute({"stop": {}})
||||||| .r1729
        try:
            self._execute({"stop": {}})
            self.process.stop()
            self.watch_stdout.stop()
            self.watch_stderr.stop()
            return self
        except Exception as cause:
            self.stop_error = cause

    def join(self):
        if self.stop_error:
            Log.error("problem with stop", cause=self.stop_error)

=======
        try:
            self._execute({"stop": {}})
            self.process.stop()
            self.watch_stdout.stop()
            self.watch_stderr.stop()
        except Exception as cause:
            self.stop_error = cause
        return self

    def join(self):
        if self.stop_error:
            logger.error("problem with stop", cause=self.stop_error)

>>>>>>> .r2071
        self.process.join()
        self.daemon.stop()
        self.errors.stop()
        return self
