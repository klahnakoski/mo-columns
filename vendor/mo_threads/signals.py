# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
# THIS THREADING MODULE IS PERMEATED BY THE please_stop SIGNAL.
# THIS SIGNAL IS IMPORTANT FOR PROPER SIGNALLING WHICH ALLOWS
# FOR FAST AND PREDICTABLE SHUTDOWN AND CLEANUP OF THREADS

from __future__ import absolute_import, division, unicode_literals

from weakref import ref

from mo_future import allocate_lock as _allocate_lock, text
from mo_logs import Log
from mo_dots import is_null
<<<<<<< .mine
||||||| .r1729
from mo_future import allocate_lock as _allocate_lock
from mo_logs import Log, Except
from mo_logs.exceptions import get_stacktrace
=======
from mo_future import allocate_lock as _allocate_lock
from mo_imports import expect
from mo_logs import logger, Except
from mo_logs.exceptions import get_stacktrace
>>>>>>> .r2071
from mo_logs.strings import quote

current_thread, threads = expect("current_thread", "threads")


DEBUG = False
MAX_NAME_LENGTH = 100


<<<<<<< .mine
||||||| .r1729
def standard_warning(cause):
    Log.warning(
        "Trigger on Signal.go() failed, and no error function provided!", cause=cause, stack_depth=1,
    )


def debug_warning(stacktrace):
    def warning(cause):
        Log.warning(
            "Trigger on Signal.go() failed, and no error function provided!",
            cause=[cause, Except(template="attached at", trace=stacktrace)],
            stack_depth=1,
        )

    return warning


=======
def standard_warning(cause):
    logger.warning(
        "Trigger on Signal.go() failed, and no error function provided!", cause=cause, stack_depth=1,
    )


def debug_warning(stacktrace):
    def warning(cause):
        logger.warning(
            "Trigger on Signal.go() failed, and no error function provided!",
            cause=[cause, Except(template="attached at", trace=stacktrace)],
            stack_depth=1,
        )

    return warning


>>>>>>> .r2071
class Signal(object):
    """
    SINGLE-USE THREAD SAFE SIGNAL (aka EVENT)

    go() - ACTIVATE SIGNAL (DOES NOTHING IF SIGNAL IS ALREADY ACTIVATED)
    wait() - PUT THREAD IN WAIT STATE UNTIL SIGNAL IS ACTIVATED
    then() - METHOD FOR OTHER THREAD TO RUN WHEN ACTIVATING SIGNAL
    """

    __slots__ = ["_name", "lock", "_go", "job_queue", "waiting_threads", "triggered_by", "__weakref__"]

    def __init__(self, name=None):
        DEBUG and name and print(f"New signal {quote(name)}")
        self._name = name
        self.lock = _allocate_lock()
        self._go = False
        self.job_queue = None
        self.waiting_threads = None
        self.triggered_by = None

    def __str__(self):
        return str(self._go)

    def __bool__(self):
        return self._go

    def __nonzero__(self):
        return self._go

    def wait(self):
        """
        PUT THREAD IN WAIT STATE UNTIL SIGNAL IS ACTIVATED
        """
        if self._go:
            return True

        thread = None
        if DEBUG:
            thread = current_thread()
            if threads.MAIN_THREAD.timers is thread:
                logger.error("Deadlock detected", stack_depth=1)

        with self.lock:
            if self._go:
                return True
            stopper = _allocate_lock()
            stopper.acquire()
            if not self.waiting_threads:
                self.waiting_threads = [(stopper, thread)]
            else:
                self.waiting_threads.append((stopper, thread))

        DEBUG and self._name and print(f"{quote(thread.name)} wait on {quote(self.name)}")
        stopper.acquire()
        DEBUG and self._name and print(f"{quote(thread.name)} released on {quote(self.name)}")
        return True

    def go(self):
        """
        ACTIVATE SIGNAL (DOES NOTHING IF SIGNAL IS ALREADY ACTIVATED)
        """
        DEBUG and self._name and print(f"requesting GO! {quote(self.name)}")

        if self._go:
            return

        with self.lock:
            if self._go:
                return
            if DEBUG:
                self.triggered_by = get_stacktrace(1)
            self._go = True

        DEBUG and self._name and print(f"GO! {quote(self.name)}")
        jobs, self.job_queue = self.job_queue, None
        stoppers, self.waiting_threads = self.waiting_threads, None

<<<<<<< .mine
        if threads:
            DEBUG and self._name and Log.note(
                "Release {{num}} threads", num=len(threads)
            )
            for t in threads:
                t.release()
||||||| .r1729
        if threads:
            DEBUG and self._name and Log.note("Release {{num}} threads", num=len(threads))
            for t in threads:
                t.release()
=======
        if stoppers:
            if DEBUG:
                if len(stoppers) == 1:
                    s, t = stoppers[0]
                    print(f"Releasing thread {quote(t.name)}")
                else:
                    print(f"Release {len(stoppers)} threads")
            for s, t in stoppers:
                s.release()
>>>>>>> .r2071

        if jobs:
            for j in jobs:
                try:
                    j()
                except Exception as cause:
                    Log.warning("Trigger on Signal.go() failed!", cause=cause)

    def then(self, target):
        """
        WARNING: THIS IS RUN BY THE timer THREAD, KEEP THIS FUNCTION SHORT, AND DO NOT BLOCK
        RUN target WHEN SIGNALED
        """
<<<<<<< .mine
        if not target:
            Log.error("expecting target")
||||||| .r1729
        if DEBUG:
            if not target:
                Log.error("expecting target")
            if isinstance(target, Signal):
                Log.error("expecting a function, not a signal")
=======
        if DEBUG:
            if not target:
                logger.error("expecting target")
            if isinstance(target, Signal):
                logger.error("expecting a function, not a signal")
>>>>>>> .r2071

        with self.lock:
            if not self._go:
                DEBUG and self._name and Log.note(
                    "Adding target to signal {{name|quote}}", name=self.name
                )

                if not self.job_queue:
                    self.job_queue = [target]
                else:
                    self.job_queue.append(target)
                return

        DEBUG and Log.note(
            "Signal {{name|quote}} already triggered, running job immediately",
            name=self.name,
        )
        target()

    def remove_go(self, target):
        """
        FOR SAVING MEMORY
        """
        with self.lock:
            if not self._go:
                try:
                    self.job_queue.remove(target)
                except ValueError:
                    pass

    @property
    def name(self):
        if not self._name:
            return "anonymous signal"
        else:
            return self._name

    def __str__(self):
        return self.name.decode(text)

    def __repr__(self):
        return text(repr(self._go))

    def __or__(self, other):
        if is_null(other):
            return self
        if not isinstance(other, Signal):
            logger.error("Expecting OR with other signal")
        if self or other:
            return DONE

        output = Signal(self.name + " | " + other.name)
        OrSignal(output, (self, other))
        return output

    def __ror__(self, other):
        return self.__or__(other)

    def __and__(self, other):
        if is_null(other) or other:
            return self
        if not isinstance(other, Signal):
            logger.error("Expecting OR with other signal")

<<<<<<< .mine
        if DEBUG and self._name:
            output = Signal(self.name + " and " + other.name)
        else:
            output = Signal(self.name + " and " + other.name)
||||||| .r1729
        if DEBUG and self._name:
            output = Signal(self.name + " & " + other.name)
        else:
            output = Signal(self.name + " & " + other.name)
=======
        name = f"{self.name} & {other.name}"
        if len(name) > MAX_NAME_LENGTH:
            name = name[:MAX_NAME_LENGTH] + "..."
        output = Signal(name)
>>>>>>> .r2071

        gen = AndSignals(output, 2)
        self.then(gen.done)
        other.then(gen.done)
        return output


class AndSignals(object):
    __slots__ = ["signal", "remaining", "locker"]

    def __init__(self, signal, count):
        """
        CALL signal.go() WHEN done() IS CALLED count TIMES
        :param signal:
        :param count:
        :return:
        """
        self.signal = signal
        self.locker = _allocate_lock()
        self.remaining = count
        if not count:
            self.signal.go()

    def done(self):
        with self.locker:
            self.remaining -= 1
            remaining = self.remaining
        if not remaining:
            self.signal.go()


<<<<<<< .mine
||||||| .r1729
def or_signal(*dependencies):
    output = Signal(" | ".join(d.name for d in dependencies))
    OrSignal(output, dependencies)
    return output


=======
def or_signal(*dependencies):
    if len(dependencies) > 5:
        name = f"{dependencies[0].name} | ({len(dependencies)} other signals)"
    else:
        name = " | ".join(d.name for d in dependencies)
    if len(name) > MAX_NAME_LENGTH:
        name = name[:MAX_NAME_LENGTH] + "..."

    output = Signal(name)
    OrSignal(output, dependencies)
    return output


>>>>>>> .r2071
class OrSignal(object):
    """
    A SELF-REFERENTIAL CLUSTER OF SIGNALING METHODS TO IMPLEMENT __or__()
    MANAGE SELF-REMOVAL UPON NOT NEEDING THE signal OBJECT ANY LONGER
    """

    __slots__ = ["signal", "dependencies"]

    def __init__(self, signal, dependencies):
        self.dependencies = dependencies
        self.signal = ref(signal, self.cleanup)
        for d in dependencies:
            d.then(self)
        signal.then(self.cleanup)

    def cleanup(self, r=None):
        for d in self.dependencies:
            d.remove_go(self)
        self.dependencies = []

    def __call__(self, *args, **kwargs):
        s = self.signal()
        if s is not None:
            s.go()


DONE = Signal()
DONE.go()
