import inspect
import logging
from datetime import datetime, timedelta
from time import perf_counter

from iss4e.util.brace_message import BraceMessage as __


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


def progress(iterable, logger=logging, level=logging.INFO, delay=5,
             msg="{verb} {countf} {name} after {timef}s ({ratef}/{avgratef} {name} per second)",
             objects="entries", verb="Processed"):
    """
    Print a short status message about the number of consumed items to `logger.level` every `delay` seconds as items are
    consumed from the given iterable.
    :return: a wrapped version of the given iterable
    """
    msg = msg.format(countf='{count:,}', timef='{time:.2f}', ratef='{rate:,.2f}', avgratef='{avgrate:,.2f}',
                     name=objects, verb=verb)

    last_print = start = perf_counter()
    last_rows = nr = 0
    val = None

    def print(last):
        nonlocal last_rows, last_print
        if (perf_counter() - last) > delay:
            logger.log(level, __(msg, count=nr, time=perf_counter() - start, value=val,
                                 rate=(nr - last_rows) / (perf_counter() - last_print),
                                 avgrate=nr / (perf_counter() - start)))
            last_print = perf_counter()
            last_rows = nr

    for nr, val in enumerate(iterable):
        # TODO should print be called before or after yielding (or both)?
        print(last_print)
        yield val
        # print(last_print)

    print(start)


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
