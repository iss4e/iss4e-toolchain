import concurrent.futures
import itertools
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import influxdb.resultset
import requests
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
from iss4e.util import AsyncLookaheadIterator
from iss4e.util import BraceMessage as __
from more_itertools import peekable

DEFAULT_BATCH_SIZE = 50000
TO_SECONDS = {
    'n': 0.000000001,
    'u': 0.000001,
    'ms': 0.001,
    's': 1,
    'm': 60,
    'h': 60 * 60
}

logger = logging.getLogger(__name__)
async_logger = logger.getChild("async")
_marker = object()
thread_pool = None


class ExtendedResultSet(influxdb.resultset.ResultSet):
    """
    An extended version of the influxdb.resultset.ResultSet that is also capable to parse the retrieved timestamps using
    the time_* configuration of InfluxDBStreamingClient
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.time_field = self.time_format = self.time_epoch = None  # injected in InfluxDBStreamingClient.query

    def _get_points_for_serie(self, serie):
        result = super()._get_points_for_serie(serie)
        if self.time_format:
            result = map(self._format_time, result)
        return result

    def _format_time(self, point):
        if self.time_field in point:
            time = point[self.time_field]
            point["__" + self.time_field] = time
            point[self.time_field] = self.time_format(time, self.time_epoch)
        return point


# make the influxdb client use our extended version of ResultSet
influxdb.resultset.ResultSet = ExtendedResultSet
influxdb.client.ResultSet = ExtendedResultSet


def series_tag_to_selector(p):
    k, v = p.split("=")
    return "{}::tag='{}'".format(k, v)


def series_tag_to_id(p):
    k, v = p.split("=")
    return str(v)


def series_tags_to_dict(tags):
    return {k: v for k, v in (tag.split("=") for tag in tags)}


def join_selectors(selectors):
    selectors = ["({})".format(w) for w in selectors if w]
    if len(selectors) == 1:
        return selectors[0]
    return " AND ".join(selectors)


class QueryStreamer(object):
    def __init__(self, query_func, query_format, batch_size):
        self.query_func = query_func
        self.query_format = query_format
        self.batch_size = itertools.repeat(batch_size)
        self.offset = itertools.count(0, batch_size)

    def __iter__(self):
        return self

    def __next__(self):
        query = self.query_format.format(offset=next(self.offset), limit=next(self.batch_size))
        before = time.perf_counter()
        async_logger.debug(" < block before")
        result = self.query_func(query)
        async_logger.debug(" > block after, blocked for {}s".format(time.perf_counter() - before))

        # peek into the result, if it is empty, we read all values from this series
        points = peekable(result.get_points())
        if not points.peek(None):
            raise StopIteration
        else:
            return points


class InfluxDBStreamingClient(InfluxDBClient):
    def __init__(self, *args, **kwargs):
        self.batched = kwargs.pop('batched', False)
        self.async_executor = kwargs.pop('async_executor', None)
        self.time_field = kwargs.pop('time_field', 'time')
        self.time_format = kwargs.pop('time_format', None)
        self.time_epoch = kwargs.pop('time_epoch', None)
        self.batch_size = kwargs.pop('batch_size', DEFAULT_BATCH_SIZE)
        super().__init__(*args, **kwargs)

    def close(self):
        self._session.close()
        if hasattr(self, 'udp_socket'):
            self.udp_socket.close()

    def list_series(self, measurement):
        # fetch all series for this measurement and parse the result
        series_res = self.query("SHOW SERIES FROM \"{}\"".format(measurement))
        series = (v['key'].split(",")[1:] for v in series_res.get_points())
        # for each series, create a WHERE clause selecting only entries from that exact series
        return [(serie, join_selectors(series_tag_to_selector(tag) for tag in serie)) for serie in series]

    def stream_measurement(self, measurement, fields=None, where="", group_order_by="", batch_size=None):
        series = self.list_series(measurement)
        # create an independent row stream for each of those selectors
        return [(sname, sselector, self.stream_params(
            measurement=measurement,
            fields=fields,
            # join series WHERE clause and WHERE clause from params
            where=join_selectors([where, sselector]),
            group_order_by=group_order_by,
            batch_size=batch_size
        )) for (sname, sselector) in series]

    def stream_params(self, measurement, fields=None, where="", group_order_by="", batch_size=None):
        if fields is None:
            fields = "*"
        elif not isinstance(fields, str):
            fields = ", ".join(fields)

        base_query = "SELECT {fields} FROM {measurement} WHERE {where} GROUP BY {group_order_by} " \
                     "LIMIT {{limit}} OFFSET {{offset}}".format(
            fields=fields, measurement=measurement,
            where=where, group_order_by=group_order_by)

        return self.stream_query(base_query, batch_size)

    def stream_query(self, query_format, batch_size=None):
        global thread_pool

        if batch_size is None:
            batch_size = self.batch_size
        streamer = QueryStreamer(self.query, query_format, batch_size)

        if self.async_executor is True:
            if not thread_pool:
                thread_pool = concurrent.futures.ThreadPoolExecutor()
            self.async_executor = thread_pool
        elif isinstance(self.async_executor, int):
            self.async_executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.async_executor)

        if self.async_executor:
            # If an async_executor is available, the iterable will be wrapped in an AsyncLookaheadIterator, that will
            # use the executor to asynchronously fetch data in the background before it is consumed from the streamer.
            streamer = AsyncLookaheadIterator(self.async_executor, streamer, logger=async_logger,
                                              warm_start=True, parallelism=2)
        if not self.batched:
            # If batched is False, the streamer of iterables will be flattened to a simple iterable.
            streamer = itertools.chain.from_iterable(streamer)
        return streamer

    def _batches(self, iterable, size):
        """The default _batched implementation can't handle iterables, only lists. This one handles both."""
        args = [iter(iterable)] * size
        iters = itertools.zip_longest(*args, fillvalue=_marker)
        return (filter(_marker.__ne__, it) for it in iters)

    def query(self, *args, **kwargs) -> ExtendedResultSet:
        time_field = kwargs.pop('time_field', self.time_field)
        time_format = kwargs.pop('time_format', self.time_format)
        time_epoch = kwargs.pop('time_epoch', self.time_epoch)

        params = kwargs.pop('params', {})
        if 'epoch' not in params or not params['epoch']:
            if time_epoch:
                params['epoch'] = time_epoch
        else:
            time_epoch = params['epoch']

        result = super().query(params=params, *args, **kwargs)
        result.time_field, result.time_format, result.time_epoch = (time_field, time_format, time_epoch)
        return result

    def drop_measurement(self, name):
        try:
            self.query("DROP MEASUREMENT \"{}\"".format(name))
            return True
        except InfluxDBClientError as e:
            if str(e).startswith('measurement not found'):
                return False
            else:
                raise

    def _write_points(self, *args, **kwargs):
        kwargs.setdefault('time_precision', self.time_epoch)
        super()._write_points(*args, **kwargs)

    def __getstate__(self):
        state = self.__dict__.copy()
        if isinstance(self.async_executor, ThreadPoolExecutor):
            if self.async_executor == thread_pool:
                state['async_executor'] = True
            else:
                state['async_executor'] = self.async_executor._max_workers
        del state['_session']
        return state

    def __setstate__(self, state):
        state['_session'] = requests.Session()
        self.__dict__.update(state)


@contextmanager
def connect(**kwargs):
    client = InfluxDBStreamingClient(**kwargs)
    start = time.perf_counter()
    try:
        yield client
    finally:
        dur = time.perf_counter() - start
        logger.debug(__("InfluxDB connection open for {:.2f}s", dur))
        client.close()
