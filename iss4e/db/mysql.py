import logging
import time
import warnings
from contextlib import contextmanager

import pymysql
from iss4e.db.mysql_stopwatch import StopwatchConnection as _Connection
from iss4e.util import BraceMessage as __
from pymysql.cursors import Cursor, DictCursor as _DictCursor, SSDictCursor as _SSDictCursor

# for Connection without Stopwatch:
# from pymysql.connections import _Connection

logger = logging.getLogger(__name__)


class QualifiedDictCursorMixin(object):
    # You can override this to use OrderedDict or other dict-like types.
    dict_type = dict

    def _do_get_result(self):
        super(QualifiedDictCursorMixin, self)._do_get_result()
        fields = []
        if self.description:
            for f in self._result.fields:
                fields.append(f.table_name + '.' + f.name)
            self._fields = fields

        if fields and self._rows:
            self._rows = [self._conv_row(r) for r in self._rows]

    def _conv_row(self, row):
        if row is None:
            return None
        return self.dict_type(zip(self._fields, row))


class QualifiedDictCursor(QualifiedDictCursorMixin, Cursor):
    """A cursor which returns results as a dictionary with keys always consisting of the fully qualified column name,
    i.e. always prefixed with the table name."""


DictCursor = _DictCursor
StreamingDictCursor = _SSDictCursor
Connection = _Connection


@contextmanager
def connect(**kwargs):
    warnings.filterwarnings('error', category=pymysql.Warning)
    connection = _Connection(**kwargs)
    start = time.perf_counter()
    try:
        yield connection
    except:
        connection.rollback()
        raise
    finally:
        dur = time.perf_counter() - start
        logger.debug(__("MySQL DB connection open for {:.2f}s", dur))
        connection.close()
