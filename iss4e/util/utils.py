import concurrent.futures
import inspect
import logging
import os
from datetime import datetime, timedelta
from queue import Empty
from time import perf_counter

from iss4e.util.brace_message import BraceMessage as __
from tabulate import tabulate


def daterange(start, stop=datetime.now(), step=timedelta(days=1)):
    """Similar to :py:func:`builtins.range`, but for dates."""
    if start < stop:
        cmp = lambda a, b: a < b
        inc = lambda a: a + step
    else:
        cmp = lambda a, b: a > b
        inc = lambda a: a - step
    yield start
    start = inc(start)
    while cmp(start, stop):
        yield start
        start = inc(start)


def zip_prev(iterable):
    """Return the sequence as tuples together with their predecessors."""
    last_val = None
    for val in iterable:
        yield (last_val, val)
        last_val = val


def _prepare_message(**kwargs):
    verb = kwargs.pop('verb ', "Processed")
    objects = kwargs.pop('objects ', "entries")
    msg = kwargs.pop('msg ', "{verb} {countf} {objects} after {timef}s ({ratef}/{avgratef} {objects} per second)")
    msg = msg.format(countf='{count:,}', timef='{time:.2f}', ratef='{rate:,.2f}', avgratef='{avgrate:,.2f}',
                     objects=objects, verb=verb)
    return msg


def _print_progress(msg, count, time, rate, avgrate, value, **kwargs):
    logger = kwargs.pop('logger', logging)
    level = kwargs.pop('level ', logging.INFO)
    logger.log(level, __(msg, count=count, time=time, rate=rate, avgrate=avgrate, value=value))


progress_counter_id = 0


def progress(iterable, delay=5, remote=None, **kwargs):
    """
    Print a short status message about the number of consumed items to `logger.level` every `delay` seconds as items are
    consumed from the given iterable.
    :return: a wrapped version of the given iterable
    """

    if remote:
        global progress_counter_id
        pid = "{}-{}".format(os.getpid(), progress_counter_id)
        progress_counter_id += 1  # use a new ID for each invocation
    msg = _prepare_message(**kwargs)
    last_print = start = perf_counter()
    last_rows = nr = 0
    val = None

    def update(last):
        nonlocal last_rows, last_print
        now = perf_counter()
        if (now - last) > delay:
            if remote:
                remote((pid, nr))
            else:
                _print_progress(msg, count=nr, time=now - start,
                                rate=((nr - last_rows) / (now - last_print)),
                                avgrate=(nr / (now - start)), value=val, **kwargs)
            last_print = now
            last_rows = nr

    if remote:  # always report status to remote at the beginning
        remote((pid, nr))
    for nr, val in enumerate(iterable):
        # TODO should update be called before or after yielding (or both)?
        update(last_print)
        yield val
        # update(last_print)
    update(start)
    if remote:  # always report status to remote at the end
        remote((pid, nr))


def async_progress(futures, queue, delay=5, **kwargs):
    futures = list(futures)  # ensure that futures can be iterated multiple times and have a length
    msg = _prepare_message(**kwargs)
    last_print = start = perf_counter()
    last_count = count = 0
    stats = {}

    def update():
        nonlocal count, last_count, last_print
        now = perf_counter()
        count = sum(stats.values())
        _print_progress("{}-{}/{} ".format(len([f for f in futures if f.done()]), len(stats), len(futures)) + msg,
                        count=count, time=now - start,
                        rate=((count - last_count) / (now - last_print)),
                        avgrate=(count / (now - start)), value=None, **kwargs)
        last_count, last_print = count, now

    for nr, future in enumerate(futures):
        while not future.done():
            # check whether any future failed
            assert all([future.result(timeout=0) or True for future in futures if future.done()])

            try:
                # wait 5 secs
                future.result(timeout=delay)
            except concurrent.futures.TimeoutError:
                pass
            while True:
                try:
                    pid, count = queue.get_nowait()
                    stats[pid] = count
                except Empty:
                    break
            update()

    update()
    logger = kwargs.get('logger', logging)
    logger.debug("Async progress finished. Stats:\n{}".format(
        tabulate(sorted(list(stats.items())), headers=["PID", "#"])))


def dump_args(frame):
    """
    Dump the name of the current function and all arguments and values passed in.
    Print this information using
        "{}(\n  {})".format(func_name, ",\n  ".join(["{} = {}".format(k, v) for k, v in arg_list])
    """
    arg_names, varargs, kwargs, values = inspect.getargvalues(frame)
    arg_list = list([(n, values[n]) for n in arg_names])
    if varargs:
        arg_list.append(("*args", varargs))
    if kwargs:
        arg_list.extend([("**" + k, v) for k, v in kwargs.items()])
    func_name = inspect.getframeinfo(frame)[2]
    return func_name, arg_list
