import itertools
import logging
import time

from iss4e.util import BraceMessage as __
from iss4e.util import progress
from pymysql import connections

logger = logging.getLogger(__name__)


class StopwatchConnection(connections.Connection):
    """Log progress information about slow-running queries mad over this connection"""

    def __init__(self, *args, **kwargs):
        connections.Connection.__init__(self, *args, **kwargs)
        self._query_start = 0
        self.result_class = StopwatchMySQLResult

    def query(self, sql, unbuffered=False):
        self._query_start = time.perf_counter()
        try:
            return connections.Connection.query(self, sql, unbuffered)
        except:
            logger.error(__("Query failed after {:.2f}s:\n{}", time.perf_counter() - self._query_start, sql))
            raise
        finally:
            self._query_start = 0

    def _read_query_result(self, unbuffered=False):
        if unbuffered:
            try:
                result = self.result_class(self)
                result.init_unbuffered_query()
            except:
                result.unbuffered_active = False
                result.connection = None
                raise
        else:
            result = self.result_class(self)
            result.read()
        self._result = result
        if result.server_status is not None:
            self.server_status = result.server_status
        return result.affected_rows


class StopwatchMySQLResult(connections.MySQLResult):
    """Most of these methods are directly copied from connections.MySQLResult,
    with just the single logging call inserted."""

    def init_unbuffered_query(self):
        self.unbuffered_active = True
        first_packet = self.connection._read_packet()
        if (time.perf_counter() - self.connection._query_start) > 2:
            logger.debug("Server took {:.2f}s for processing request, rows will be loaded asynchronously"
                         .format(time.perf_counter() - self.connection._query_start))

        if first_packet.is_ok_packet():
            self._read_ok_packet(first_packet)
            self.unbuffered_active = False
            self.connection = None
        elif first_packet.is_load_local_packet():
            self._read_load_local_packet(first_packet)
            self.unbuffered_active = False
            self.connection = None
        else:
            self.field_count = first_packet.read_length_encoded_integer()
            self._get_descriptions()

            # Apparently, MySQLdb picks this number because it's the maximum
            # value of a 64bit unsigned integer. Since we're emulating MySQLdb,
            # we set it to this instead of None, which would be preferred.
            self.affected_rows = 18446744073709551615

    def read(self):
        try:
            first_packet = self.connection._read_packet()
            if (time.perf_counter() - self.connection._query_start) > 2:
                logger.debug("Server took {:.2f}s for processing request, loading rows now"
                             .format(time.perf_counter() - self.connection._query_start))

            if first_packet.is_ok_packet():
                self._read_ok_packet(first_packet)
            elif first_packet.is_load_local_packet():
                self._read_load_local_packet(first_packet)
            else:
                self._read_result_packet(first_packet)
        finally:
            self.connection = None

    def _read_rowdata_packet(self):
        """Read a rowdata packet for each data row in the result set."""
        rows = []
        for _ in progress(itertools.count(), logger=logger, level=logging.DEBUG, verb="Got",
                          objects="rows"):  # == while True
            packet = self.connection._read_packet()
            if self._check_packet_is_eof(packet):
                break
            rows.append(self._read_row_from_packet(packet))

        self.affected_rows = len(rows)
        self.rows = tuple(rows)
        dur = time.perf_counter() - self.connection._query_start
        if dur > 4:
            logger.debug(__("Took {:.2f}s for executing query affecting {:,} rows", dur, len(rows)))
        self.connection = None  # release reference to kill cyclic reference.
