import itertools
import logging
import time
from contextlib import contextmanager

import influxdb.resultset
from influxdb import InfluxDBClient
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


def escape_series_tag(p):
    k, v = p.split("=")
    return "{}='{}'".format(k, v)


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
        super().__init__(*args, **kwargs)

    def close(self):
        self._session.close()
        if hasattr(self, 'udp_socket'):
            self.udp_socket.close()

    def stream_series(self, measurement, fields=None, where="", group_order_by="", batch_size=DEFAULT_BATCH_SIZE):
        # fetch all series for this measurement and parse the result
        series_res = self.query("SHOW SERIES FROM \"{}\"".format(measurement))
        series = (v['key'].split(",")[1:] for v in series_res.get_points())
        # for each series, create a WHERE clause selecting only entries from that exact series
        series_selectors = [" AND ".join(escape_series_tag(v) for v in a) for a in series]
        # and create and independent row stream for each of those selectors
        series_stream = [self.stream_params(
            measurement=measurement,
            fields=fields,
            # join series WHERE clause and WHERE clause from params
            selector=" AND ".join("({})".format(w) for w in [where, sselector] if w),
            group_order_by=group_order_by,
            batch_size=batch_size
        ) for sselector in series_selectors]

        return zip(series_selectors, series_stream)

    def stream_params(self, measurement, fields=None, selector="", group_order_by="", batch_size=DEFAULT_BATCH_SIZE):
        if fields is None:
            fields = "*"
        elif not isinstance(fields, str):
            fields = ", ".join(fields)

        base_query = "SELECT {fields} FROM {measurement} WHERE {where} {group_order_by} " \
                     "LIMIT {{limit}} OFFSET {{offset}}".format(
            fields=fields, measurement=measurement,
            where=selector, group_order_by=group_order_by)

        return self.stream_query(base_query, batch_size)

    def stream_query(self, query_format, batch_size):
        streamer = QueryStreamer(self.query, query_format, batch_size)

        if self.async_executor:
            # If an async_executor is available, the iterable will be wrapped in an AsyncLookaheadIterator, that will
            # use the executor to asynchronously fetch data in the background before it is consumed from the streamer.
            streamer = AsyncLookaheadIterator(self.async_executor, streamer, logger=async_logger, warm_start=True)
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
