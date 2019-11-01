import datetime
import json
import logging
import queue
import re
import time
from typing import List

import pyodbc
from tabulate import tabulate

from cdc_kafka import validation
from . import kafka, tracked_tables, constants, metric_reporters, options

logger = logging.getLogger(__name__)


def run() -> None:
    opts, reporters = options.get_options_and_reporters()

    logger.debug('Parsed configuration: \n%s', json.dumps(vars(opts), indent=4))
    if not (opts.schema_registry_url and opts.kafka_bootstrap_servers and opts.db_conn_string):
        raise Exception('Arguments schema_registry_url, kafka_bootstrap_servers, and db_conn_string are all required.')

    # The Linux ODBC driver doesn't do failover, so we're hacking it in here. This will only work for initial
    # connections. If a failover happens while this process is running, the app will crash. Have a process supervisor
    # that can restart it if that happens, and it'll connect to the failover on restart:
    # THIS ASSUMES that you are using the exact keywords 'SERVER' and 'Failover_Partner' in your connection string!
    try:
        db_conn = pyodbc.connect(opts.db_conn_string, autocommit=True)
    except pyodbc.ProgrammingError as e:
        if e.args[0] != '42000':
            raise
        server = re.match(r".*SERVER=(.*?);", opts.db_conn_string)
        failover_partner = re.match(r".*Failover_Partner=(.*?);", opts.db_conn_string)
        if failover_partner is not None and server is not None:
            failover_partner = failover_partner.groups(1)[0]
            server = server.groups(1)[0]
            opts.db_conn_string = opts.db_conn_string.replace(server, failover_partner)
            db_conn = pyodbc.connect(opts.db_conn_string, autocommit=True)

    with kafka.KafkaClient(opts.kafka_bootstrap_servers,
                           opts.schema_registry_url,
                           opts.kafka_timeout_seconds,
                           opts.progress_topic_name,
                           tracked_tables.TrackedTable.capture_instance_name_resolver,
                           opts.extra_kafka_consumer_config,
                           opts.extra_kafka_producer_config) as kafka_client, db_conn:

        tables = tracked_tables.build_tracked_tables_from_cdc_metadata(db_conn,
                                                                       opts.topic_name_template,
                                                                       opts.table_whitelist_regex,
                                                                       opts.table_blacklist_regex,
                                                                       opts.snapshot_table_whitelist_regex,
                                                                       opts.snapshot_table_blacklist_regex,
                                                                       opts.capture_instance_version_strategy,
                                                                       opts.capture_instance_version_regex)

        determine_start_points_and_finalize_tables(
            kafka_client, tables, opts.lsn_gap_handling, opts.partition_count, opts.replication_factor,
            opts.extra_topic_config, opts.progress_csv_path, opts.run_validations)

        if opts.run_validations:
            validator = validation.Validator(kafka_client, db_conn, tables)
            ok = validator.run()
            exit(ok)

        logger.info('Beginning processing for %s tracked tables.', len(tables))

        # Prioritization in this queue is based on the commit time of the change data rows pulled from SQL. It will
        # hold one entry per tracked table, which will be replaced with the next entry for that table after the
        # current one is pulled off:
        pq = queue.PriorityQueue(len(tables))

        for table in tables:
            priority_tuple, msg_key, msg_value = table.pop_next()
            pq.put((priority_tuple, msg_key, msg_value, table))

        metrics_interval = datetime.timedelta(seconds=opts.metric_reporting_interval)
        last_metrics_emission_time = datetime.datetime.now()
        last_published_change_msg_db_time = datetime.datetime.now() - datetime.timedelta(days=1)
        metrics_accum = metric_reporters.MetricsAccumulator(db_conn)
        pop_time_total, produce_time_total, publish_count = 0.0, 0.0, 0

        try:
            while True:
                if (datetime.datetime.now() - last_metrics_emission_time) > metrics_interval:
                    metrics_accum.determine_lags(last_published_change_msg_db_time, any([t.lagging for t in tables]))
                    for reporter in reporters:
                        reporter.emit(metrics_accum)
                    last_metrics_emission_time = datetime.datetime.now()
                    metrics_accum = metric_reporters.MetricsAccumulator(db_conn)
                    if publish_count:
                        logger.debug('Timings per msg: DB (pop) - %s us, Kafka (produce/commit) - %s us',
                                     int(pop_time_total / publish_count * 1000000),
                                     int(produce_time_total / publish_count * 1000000))

                priority_tuple, msg_key, msg_value, table = pq.get()

                if msg_key is not None:
                    start_time = time.perf_counter()
                    kafka_client.produce(
                        table.topic_name, msg_key, table.key_schema_id, msg_value, table.value_schema_id)
                    produce_time_total += (time.perf_counter() - start_time)
                    publish_count += 1
                    metrics_accum.record_publish += 1
                    if msg_value and msg_value[constants.OPERATION_NAME] != constants.SNAPSHOT_OPERATION_NAME:
                        last_published_change_msg_db_time = priority_tuple[0]

                    if msg_value[constants.OPERATION_NAME] == 'Delete' and not opts.disable_deletion_tombstones:
                        start_time = time.perf_counter()
                        kafka_client.produce(
                            table.topic_name, msg_key, table.key_schema_id, None, table.value_schema_id)
                        produce_time_total += (time.perf_counter() - start_time)
                        publish_count += 1
                        metrics_accum.tombstone_publish += 1

                kafka_client.commit_progress()

                # Put the next entry for the table just handled back on the priority queue:
                start_time = time.perf_counter()
                priority_tuple, msg_key, msg_value = table.pop_next()
                pop_time_total += (time.perf_counter() - start_time)
                pq.put((priority_tuple, msg_key, msg_value, table))
        except KeyboardInterrupt:
            logger.info('Exiting due to external interrupt.')
            exit(0)


def determine_start_points_and_finalize_tables(
        kafka_client: kafka.KafkaClient, tables: List[tracked_tables.TrackedTable], lsn_gap_handling: str,
        partition_count: int, replication_factor: int, extra_topic_config: str, progress_csv_path: str = None,
        validation_mode: bool = False) -> None:
    topic_names = [t.topic_name for t in tables]

    if validation_mode:
        for table in tables:
            table.snapshot_allowed = False
            table.finalize_table(constants.BEGINNING_CHANGE_TABLE_INDEX, {}, lsn_gap_handling,
                                 kafka_client.register_schemas)
        return

    watermarks_by_topic = kafka_client.get_topic_watermarks(topic_names)
    first_check_watermarks_json = json.dumps(watermarks_by_topic, indent=4)

    logger.info('Pausing briefly to ensure target topics are not receiving new messages from elsewhere...')
    time.sleep(constants.STABLE_WATERMARK_CHECKS_INTERVAL_SECONDS)

    watermarks_by_topic = kafka_client.get_topic_watermarks(topic_names)
    second_check_watermarks_json = json.dumps(watermarks_by_topic, indent=4)

    if first_check_watermarks_json != second_check_watermarks_json:
        raise Exception(f'Watermarks for one or more target topics changed between successive checks. '
                        f'Another process may be producing to the topic(s). Bailing.\nFirst check: '
                        f'{first_check_watermarks_json}\nSecond check: {second_check_watermarks_json}')

    prior_progress = kafka_client.get_prior_progress_or_create_progress_topic(progress_csv_path)
    prior_progress_log_table_data = []

    for table in tables:
        if table.topic_name not in watermarks_by_topic:
            logger.info('Creating topic %s', table.topic_name)
            kafka_client.create_topic(table.topic_name, partition_count, replication_factor, extra_topic_config)
            start_change_index, start_snapshot_value = None, None
        else:
            prior_change_rows_progress = prior_progress.get((
                ('capture_instance_name', table.capture_instance_name),
                ('progress_kind', constants.CHANGE_ROWS_PROGRESS_KIND),
                ('topic_name', table.topic_name)
            ))
            prior_snapshot_rows_progress = prior_progress.get((
                ('capture_instance_name', table.capture_instance_name),
                ('progress_kind', constants.SNAPSHOT_ROWS_PROGRESS_KIND),
                ('topic_name', table.topic_name)
            ))

            start_change_index = prior_change_rows_progress and tracked_tables.ChangeTableIndex(
                prior_change_rows_progress['last_ack_change_table_lsn'],
                prior_change_rows_progress['last_ack_change_table_seqval'],
                prior_change_rows_progress['last_ack_change_table_operation']
            )
            start_snapshot_value = prior_snapshot_rows_progress and \
                prior_snapshot_rows_progress['last_ack_snapshot_key_field_values']

        table.finalize_table(start_change_index or constants.BEGINNING_CHANGE_TABLE_INDEX,
                             start_snapshot_value, lsn_gap_handling, kafka_client.register_schemas)

        if not table.snapshot_allowed:
            snapshot_state = '<not doing>'
        elif table.snapshot_complete:
            snapshot_state = '<already complete>'
        elif table.last_read_key_for_snapshot_display is None:
            snapshot_state = '<from beginning>'
        else:
            snapshot_state = f'From {table.last_read_key_for_snapshot_display}'

        prior_progress_log_table_data.append((table.capture_instance_name, table.fq_name, table.topic_name,
                                              start_change_index or '<from beginning>', snapshot_state))

    headers = ('Capture instance name', 'Source table name', 'Topic name', 'From change table index', 'Snapshots')
    table = tabulate(prior_progress_log_table_data, headers, tablefmt='fancy_grid')

    logger.info('Processing will proceed from the following positions based on the last message from each topic '
                'and/or the snapshot progress committed in Kafka (NB: snapshot reads occur BACKWARDS from high to '
                'low key column values):\n%s', table)
