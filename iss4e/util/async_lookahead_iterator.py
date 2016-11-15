import collections
import logging
from time import perf_counter

my_logger = logging.getLogger(__name__)


class AsyncLookaheadIterator(object):
    def __init__(self, executor, iterable, logger=my_logger, warm_start=False, parallelism=4):
        self._log = logger
        self._exec = executor
        self._it = iter(iterable)
        self._pending = collections.deque()
        self._parallelism = parallelism
        self._submit_count = self._run_count = 0
        if warm_start:
            self.__fill_queue()

    def __iter__(self):
        return self

    def __fill_queue(self):
        while self._pending is not None and len(self._pending) < self._parallelism:
            self._pending.append(self._exec.submit(self.__get_next, self._submit_count))
            self._submit_count += 1

    def __get_next(self, nr):
        assert nr == self._run_count, "Task nr {} out of order (max is {})".format(nr, self._run_count)
        self._run_count += 1
        return self._it.__next__()

    def __next__(self):
        # make sure the queue is filled at the beginning
        self.__fill_queue()

        # return the last result and schedule the next execution in the background
        before = perf_counter()
        self._log.debug("[  wait before")
        try:
            result = self._pending.popleft().result()
            self._log.debug("]  wait after, waited for {}s".format(perf_counter() - before))
            self.__fill_queue()
            return result
        except StopIteration:
            self._log.debug("]  wait stopped at end, waited for {}s".format(perf_counter() - before))
            self._pending = None
            raise
