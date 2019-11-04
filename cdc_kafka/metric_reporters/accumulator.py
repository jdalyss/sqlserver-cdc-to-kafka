import datetime
import socket
from typing import Any, Dict, List

import pyodbc
import sortedcontainers

from cdc_kafka import constants, confluent_kafka


class MetricsAccumulator(object):
    def __init__(self, db_conn: pyodbc.Connection, metrics_namespace: str=None, hostname: str=None):
        self._db_conn = db_conn
        self._hostname: str = hostname or socket.getfqdn()
        self._metrics_namespace: str = metrics_namespace or self._hostname

    def reset_and_start(self):
        self._start_time_utc: datetime.datetime = datetime.datetime.utcnow()

        self._change_data_queries_cnt: int = 0
        self._change_data_queries_cum_time: datetime.timedelta = datetime.timedelta(0)
        self._change_data_queries_rows_retrieved_cnt: int = 0

        self._snapshot_queries_cnt: int = 0
        self._snapshot_queries_cum_time: datetime.timedelta = datetime.timedelta(0)
        self._snapshot_queries_rows_retrieved_cnt: int = 0

        self._change_lsns_produced: sortedcontainers.SortedList = sortedcontainers.SortedList()
        self._change_tran_end_times_produced = sortedcontainers.SortedList()
        self._e2e_latencies: List[datetime.timedelta] = []

        self._kafka_produce_cum_time: datetime.timedelta = datetime.timedelta(0)
        self._kafka_commit_cum_time: datetime.timedelta = datetime.timedelta(0)
        self._kafka_commit_cnt: int = 0
        self._kafka_delivery_acks_cnt: int = 0

        self._deletes_produced_cnt: int = 0
        self._inserts_produced_cnt:int = 0
        self._progress_produced_cnt: int = 0
        self._snapshots_produced_cnt: int = 0
        self._tombstones_produced_cnt: int = 0
        self._updates_produced_cnt: int = 0

    def end_and_get_values(self):
        end_time = datetime.datetime.utcnow()
        db_cum_time_secs = (self._snapshot_queries_cum_time + self._change_data_queries_cum_time).total_seconds()
        db_queries_cnt = self._snapshot_queries_cnt + self._change_data_queries_cnt
        kafka_produce_cnt = self._deletes_produced_cnt + self._inserts_produced_cnt + self._progress_produced_cnt + \
            self._snapshots_produced_cnt + self._tombstones_produced_cnt + self._updates_produced_cnt
        accounted_time_secs = db_cum_time_secs + self._kafka_produce_cum_time.total_seconds() + \
            self._kafka_commit_cum_time.total_seconds()

        self._set_db_lag_and_clock_skew()

        return {
            'metrics_namespace': self._metrics_namespace,
            'hostname': self._hostname,
            'interval_start_utc': self._start_time_utc,
            'interval_end_utc': end_time,
            'interval_delta_secs': (end_time - self._start_time_utc).total_seconds(),
            'interval_earliest_change_lsn_produced': (self._change_lsns_produced and self._change_lsns_produced[0]) or None,
            'interval_latest_change_lsn_produced': (self._change_lsns_produced and self._change_lsns_produced[-1]) or None,
            'interval_earliest_change_tran_end_time_produced': (self._change_tran_end_times_produced and self._change_tran_end_times_produced[0]) or None,
            'interval_latest_change_tran_end_time_produced': (self._change_tran_end_times_produced and self._change_tran_end_times_produced[-1]) or None,
            'e2e_lag_secs_min': (self._e2e_latencies and min(self._e2e_latencies)) or None,
            'e2e_lag_secs_max': (self._e2e_latencies and max(self._e2e_latencies)) or None,
            'e2e_lag_secs_avg': (self._e2e_latencies and sum(self._e2e_latencies) / len(self._e2e_latencies)) or None,
            'db_cdc_lag_secs': self._db_cdc_lag_seconds,
            'cdc_to_kafka_app_lag_secs': (self._e2e_latencies and self._e2e_latencies[-1] - self._db_cdc_lag_seconds) or None,
            'kafka_produce_count_total': self._kafka_produce_cnt,
            'kafka_produce_time_cum_secs': self._kafka_produce_cum_time.total_seconds(),
            'kafka_produce_time_avg_per_record_micros': (self._kafka_produce_cnt and self._kafka_produce_cum_time.total_seconds() / self._kafka_produce_cnt * 1000000) or 0,
            'tombstone_records_produced': self._tombstones_produced_cnt,
            'delete_changes_produced': self._deletes_produced_cnt,
            'insert_changes_produced': self._inserts_produced_cnt,
            'update_changes_produced': self._updates_produced_cnt,
            'snapshot_records_produced': self._snapshots_produced_cnt,
            'progress_records_produced': self._progress_produced_cnt,
            'kafka_progress_commit_and_flush_count': self._kafka_commit_cnt,
            'kafka_progress_commit_and_flush_time_cum_secs': self._kafka_commit_cum_time.total_seconds(),
            'kafka_progress_commit_and_flush_time_avg_time_secs': (self._kafka_commit_cnt and self._kafka_commit_cum_time.total_seconds() / self._kafka_commit_cnt) or 0,
            'kafka_delivery_acks_count': self._kafka_delivery_acks_cnt,
            'db_total_query_count': self._snapshot_queries_cnt + self._change_data_queries_cnt,
            'db_total_rows_retrieved': self._snapshot_queries_rows_retrieved_cnt + self._change_data_queries_rows_retrieved_cnt,
            'db_total_query_time_cum_secs': db_cum_time_secs,
            'db_avg_time_per_query_ms': (db_queries_cnt and db_cum_time_secs / db_queries_cnt * 1000) or 0,
            'db_snapshot_query_count': self._snapshot_queries_cnt,
            'db_snapshot_rows_retrieved':  self._snapshot_queries_rows_retrieved_cnt,
            'db_snapshot_query_time_cum_secs': self._snapshot_queries_cum_time.total_seconds(),
            'db_avg_time_per_snapshot_query_ms': (self._snapshot_queries_cnt and self._snapshot_queries_cum_time.total_seconds() / self._snapshot_queries_cnt * 1000) or 0,
            'db_change_tables_query_count': self._change_data_queries_cnt,
            'db_change_table_rows_retrieved': self._change_data_queries_rows_retrieved_cnt,
            'db_change_tables_query_time_cum_secs': self._change_data_queries_cum_time.total_seconds(),
            'db_avg_time_per_change_table_query_ms': (self._change_data_queries_cnt and self._change_data_queries_cum_time.total_seconds() / self._change_data_queries_cnt * 1000) or 0,
            'misc_unaccounted_time': (end_time - self._start_time_utc - self._kafka_produce_cum_time - self._kafka_commit_cum_time - TODO).total_seconds(),
        }


    def register_db_query(self, time_taken, is_snapshot, rows_retrieved):
        if is_snapshot:
            self._snapshot_queries_cnt += 1
            self._snapshot_queries_rows_retrieved_cnt += rows_retrieved
            self._snapshot_queries_cum_time += time_taken
        else:
            self._change_data_queries_cnt += 1
            self._change_data_queries_rows_retrieved_cnt += rows_retrieved
            self._change_data_queries_cum_time += time_taken

    def register_kafka_produce(self, time_taken: datetime.timedelta, message_value: Dict[str, Any]):
        self._kafka_produce_cnt += 1
        self._kafka_produce_cum_time += time_taken

        if message_value is None
            self._tombstones_produced_cnt += 1
            return

        operation_name = message_value[constants.OPERATION_NAME]

        if operation_name == constants.SNAPSHOT_OPERATION_NAME:
            self._snapshots_produced_cnt += 1
            return

        self._change_lsns_produced.add(message_value[constants.LSN_NAME])
        self._change_tran_end_times_produced.add(message_value[constants.TRAN_END_TIME_NAME])

        if operation_name == constants.DELETE_OPERATION_NAME:
            self._deletes_produced_cnt += 1
        elif operation_name == constants.INSERT_OPERATION_NAME:
            self._inserts_produced_cnt += 1
        elif operation_name == constants.POST_UPDATE_OPERATION_NAME:
            self._updates_produced_cnt += 1
        else:
            raise Exception(f'Unrecognized operation name: {operation_name}')

    def register_kafka_commit(self, time_taken):
        self._kafka_commit_cnt += 1
        self._kafka_commit_cum_time += time_taken

    def register_kafka_delivery_callback(self,  message_value: Dict[str, Any]):
        self._kafka_delivery_acks_cnt += 1
        if message_value[constants.OPERATION_NAME] != constants.SNAPSHOT_OPERATION_NAME:
            e2e_latency = (datetime.datetime.utcnow() + self.db_clock_skew -
                           message_value[constants.TRAN_END_TIME_NAME]).total_seconds()
            self._e2e_latencies.append(e2e_latency)

    def _set_db_lag_and_clock_skew(self):
        self._cursor.execute(constants.LAG_QUERY)
        db_cdc_latest_tran_end_time, db_now = self._cursor.fetchone()
        self.db_clock_skew = db_now - datetime.datetime.utcnow()
        self._db_cdc_lag_seconds = (db_now - db_cdc_latest_tran_end_time).total_seconds()
