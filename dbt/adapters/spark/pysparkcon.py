from __future__ import annotations

from datetime import datetime

# from types import TracebackType
from typing import Any

from dbt.adapters.events.logging import AdapterLogger
from dbt_common.utils.encoding import DECIMALS
from dbt_common.exceptions import DbtDatabaseError

from pyspark.sql import SparkSession


logger = AdapterLogger("Spark")
NUMBERS = DECIMALS + (int, float)


class PysparkConnectionWrapper(object):
    """Wrap a Spark context"""

    def __init__(self, handle=None) -> None:  # type: ignore[no-untyped-def]
        self.handle = handle
        self._cursor = None
        self.result = None
        self.spark = SparkSession._activeSession

    def cursor(self) -> Any:
        return self

        def cancel(self):
            if self._cursor:
                # Handle bad response in the pyhive lib when
                # the connection is cancelled
                try:
                    self._cursor.cancel()
                except EnvironmentError as exc:
                    logger.debug("Exception while cancelling query: {}".format(exc))

    def close(self) -> None:
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.close()
            except EnvironmentError as exc:
                logger.debug("Exception while closing cursor: {}".format(exc))

    def rollback(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        logger.debug("NotImplemented: rollback")

    def execute(self, sql, bindings=None):  # type: ignore[no-untyped-def]
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        if bindings is not None:
            bindings = [self._fix_binding(binding) for binding in bindings]
            sql = sql % tuple(bindings)
        logger.debug(f"execute sql:{sql}")
        try:
            self.result = self.spark.sql(sql)
            logger.debug("Executed with no errors")
        except Exception as e:
            logger.debug(f"raising error {e}")
            raise DbtDatabaseError(e)

    @classmethod
    def _fix_binding(cls, value):  # type: ignore[no-untyped-def]
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, datetime):
            return "'" + value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "'"
        elif isinstance(value, str):
            return "'" + value + "'"
        else:
            logger.debug(type(value))
            return "'" + str(value) + "'"
