import time
import datetime

import pendulum
import singer
from singer import metadata
from singer.bookmarks import write_bookmark, reset_stream
from ratelimit import limits, sleep_and_retry, RateLimitException
from backoff import on_exception, expo, constant

from .schemas import (
    IDS
)
from .http import MetricsRateLimitException

LOGGER = singer.get_logger()

MAX_METRIC_JOB_TIME = 1800
METRIC_JOB_POLL_SLEEP = 3

def count(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))

def write_records(tap_stream_id, records):
    singer.write_records(tap_stream_id, records)
    count(tap_stream_id, records)

def get_date_and_integer_fields(stream):
    date_fields = []
    integer_fields = []
    for prop, json_schema in stream.schema.properties.items():
        _type = json_schema.type
        if isinstance(_type, list) and 'integer' in _type or \
            _type == 'integer':
            integer_fields.append(prop)
        elif json_schema.format == 'date-time':
            date_fields.append(prop)
    return date_fields, integer_fields

def base_transform(date_fields, integer_fields, obj):
    new_obj = {}
    for field, value in obj.items():
        if value == '':
            value = None
        elif field in integer_fields and value is not None:
            value = int(value)
        elif field in date_fields and value is not None:
            value = pendulum.parse(value).isoformat()
        new_obj[field] = value
    return new_obj

# not using this for now since we're only initially building this for the team_table
# and want all its data, but will leave in for further enhancement
def select_fields(mdata, obj):
    new_obj = {}
    for key, value in obj.items():
        field_metadata = mdata.get(('properties', key))
        if field_metadata and \
            (field_metadata.get('selected') is True or \
            field_metadata.get('inclusion') == 'automatic'):
            new_obj[key] = value
    return new_obj

@on_exception(constant, MetricsRateLimitException, max_tries=5, interval=60)
@on_exception(expo, RateLimitException, max_tries=5)
@sleep_and_retry
@limits(calls=1, period=61) # 60 seconds needed to be padded by 1 second to work
def get_metric(atx, metric, start_date, end_date):
    LOGGER.info('Metrics query - metric: {} start_date: {} end_date: {} '.format(
        metric,
        start_date,
        end_date))
    return atx.client.get('/analytics', params={'start': start_date, \
            'end': end_date, 'metrics[]':metric}, endpoint='analytics')

def sync_metric(atx, metric, incremental_range, start_date, end_date):
    with singer.metrics.job_timer('daily_aggregated_metric'):
        start = time.monotonic()
        start_date_formatted = datetime.datetime.utcfromtimestamp(start_date).strftime('%Y-%m-%d')
        # we've really moved this functionality to the request in the http script
        #so we don't expect that this will actually have to run mult times
        while True:
            if (time.monotonic() - start) >= MAX_METRIC_JOB_TIME:
                raise Exception('Metric job timeout ({} secs)'.format(
                    MAX_METRIC_JOB_TIME))
            data = get_metric(atx, metric, start_date, end_date)
            if data != '':
                break
            else:
                time.sleep(METRIC_JOB_POLL_SLEEP)

    data_rows = []
    # transform the team_table data
    if metric == 'team_table':
        for row in data:

            # One of the row returned from frontapp is an aggregate row
            # and has a slightly different form
            if not 'url' in row[0]:
                row[0]['url'] = ""

            if not 'id' in row[0]:
                row[0]['id'] = 0

            if not 'p' in row[0]:
                row[0]['p'] = row[0]['v']

            data_rows.append({
                "analytics_date": start_date_formatted,
                "analytics_range": incremental_range,
                "teammate_v": row[0]['v'],
                "teammate_url": row[0]['url'],
                "teammate_id": row[0]['id'],
                "teammate_p": row[0]['p'],
                "num_conversations_v": row[1]['v'],
                "num_conversations_p": row[1]['p'],
                "avg_message_conversations_v": row[2]['v'],
                "avg_message_conversations_p": row[2]['p'],
                "avg_reaction_time_v": row[3]['v'],
                "avg_reaction_time_p": row[3]['p'],
                "avg_first_reaction_time_v": row[4]['v'],
                "avg_first_reaction_time_p": row[4]['p'],
                "num_messages_v": row[5]['v'],
                "num_messages_p": row[5]['p'],
                "num_sent_v": row[6]['v'],
                "num_sent_p": row[6]['p'],
                "num_replied_v": row[7]['v'],
                "num_replied_p": row[7]['p'],
                "num_composed_v": row[8]['v'],
                "num_composed_p": row[8]['p']
                })

    # transform the team_table data
    if metric == 'tags_table':
        for row in data:
            data_rows.append({
                "analytics_date": start_date_formatted,
                "analytics_range": incremental_range,
                "tag_v": row[0]['v'],
                "tag_url": row[0]['url'],
                "tag_id": row[0]['id'],
                "conversations_archived_v": row[1]['v'],
                "conversations_archived_p": row[1]['p'],
                "conversations_open_v": row[2]['v'],
                "conversations_open_p": row[2]['p'],
                "conversations_total_v": row[3]['v'],
                "conversations_total_p": row[3]['p'],
                "num_messages_v": row[4]['v'],
                "num_messages_p": row[4]['p'],
                "avg_message_conversations_v": row[5]['v'],
                "avg_message_conversations_p": row[5]['p']
                })

    # transform the customers_table data
    if metric == 'customers_table':
        for row in data:

            # Some resource (ex. of type 'contact') don't have URLs
            if not 'url' in row[0]:
                row[0]['url'] = None

            data_rows.append({
                "analytics_date": start_date_formatted,
                "analytics_range": incremental_range,
                "resource_t": row[0]['t'],
                "resource_v": row[0]['v'],
                "resource_id": row[0]['id'],
                "resource_url": row[0]['url'],
                "num_received_v": row[1]['v'],
                "num_received_p": row[1]['p'],
                "num_sent_v": row[2]['v'],
                "num_sent_p": row[2]['p'],
                "avg_first_response_v": row[3]['v'],
                "avg_first_response_p": row[3]['p'],
                "avg_response_v": row[4]['v'],
                "avg_response_p": row[4]['p'],
                "avg_resolution_v": row[5]['v'],
                "avg_resolution_p": row[5]['p']
                })

    # transform the first_response_histo data
    if metric == 'first_response_histo':
        for row in data:
            data_rows.append({
                "analytics_date": start_date_formatted,
                "analytics_range": incremental_range,
                "time_v": row[0]['v'],
                "replies_v": row[1]['v'],
                "replies_p": row[1]['p']
                })

    # transform the resolution_histo data
    if metric == 'resolution_histo':
        for row in data:
            data_rows.append({
                "analytics_date": start_date_formatted,
                "analytics_range": incremental_range,
                "time_v": row[0]['v'],
                "resolutions_v": row[1]['v'],
                "resolutions_p": row[1]['p']
                })

    # transform the response_histo data
    if metric == 'response_histo':
        for row in data:
            data_rows.append({
                "analytics_date": start_date_formatted,
                "analytics_range": incremental_range,
                "time_v": row[0]['v'],
                "replies_v": row[1]['v'],
                "replies_p": row[1]['p']
                })

    # transform the top_conversations_table data
    if metric == 'top_conversations_table':
        for row in data:
            data_rows.append({
                "analytics_date": start_date_formatted,
                "analytics_range": incremental_range,
                "teammate_v": row[0]['v'],
                "teammate_url": row[0]['url'],
                "teammate_id": row[0]['id'],
                "num_conversations_v": row[1]['v'],
                "num_conversations_p": row[1]['p']
                })

    # transform the top_reaction_time_table data
    if metric == 'top_reaction_time_table':
        for row in data:
            data_rows.append({
                "analytics_date": start_date_formatted,
                "analytics_range": incremental_range,
                "teammate_v": row[0]['v'],
                "teammate_url": row[0]['url'],
                "teammate_id": row[0]['id'],
                "avg_reaction_time_v": row[1]['v'],
                "avg_reaction_time_p": row[1]['p']
                })

    # transform the top_replies_table data
    if metric == 'top_replies_table':
        for row in data:
            data_rows.append({
                "analytics_date": start_date_formatted,
                "analytics_range": incremental_range,
                "teammate_v": row[0]['v'],
                "teammate_url": row[0]['url'],
                "teammate_id": row[0]['id'],
                "num_replies_v": row[1]['v'],
                "num_replies_p": row[1]['p']
                })

    write_records(metric, data_rows)

def write_metrics_state(atx, metric, date_to_resume):
    write_bookmark(atx.state, metric, 'date_to_resume', date_to_resume.to_datetime_string())
    atx.write_state()

def sync_metrics(atx, metric):
    incremental_range = atx.config.get('incremental_range')
    stream = atx.catalog.get_stream(metric)
    bookmark = atx.state.get('bookmarks', {}).get(metric, {})

    LOGGER.info('metric: {} '.format(metric))
    mdata = metadata.to_map(stream.metadata)

    # start_date is defaulted in the config file 2018-01-01
    # if there's no default date and it gets set to now, then start_date will have to be
    #   set to the prior business day/hour before we can use it.
    now = datetime.datetime.now()
    if incremental_range == "daily":
        s_d = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = pendulum.parse(atx.config.get('start_date', s_d + datetime.timedelta(days=-1, hours=0)))
    elif incremental_range == "hourly":
        s_d = now.replace(minute=0, second=0, microsecond=0)
        start_date = pendulum.parse(atx.config.get('start_date', s_d + datetime.timedelta(days=0, hours=-1)))
    LOGGER.info('start_date: {} '.format(start_date))


    # end date is not usually specified in the config file by default so end_date is now.
    # if end date is now, we will have to truncate them
    # to the nearest day/hour before we can use it.
    if incremental_range == "daily":
        e_d = now.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        end_date = pendulum.parse(atx.config.get('end_date', e_d))
    elif incremental_range == "hourly":
        e_d = now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        end_date = pendulum.parse(atx.config.get('end_date', e_d))
    LOGGER.info('end_date: {} '.format(end_date))

    # if the state file has a date_to_resume, we use it as it is.
    # if it doesn't exist, we overwrite by start date
    s_d = start_date.strftime("%Y-%m-%d %H:%M:%S")
    last_date = pendulum.parse(bookmark.get('date_to_resume', s_d))
    LOGGER.info('last_date: {} '.format(last_date))


    # no real reason to assign this other than the naming
    # makes better sense once we go into the loop
    current_date = last_date

    while current_date <= end_date:
        if incremental_range == "daily":
            next_date = current_date + datetime.timedelta(days=1, hours=0)
        elif incremental_range == "hourly":
            next_date = current_date + datetime.timedelta(days=0, hours=1)

        ut_current_date = int(current_date.timestamp())
        LOGGER.info('ut_current_date: {} '.format(ut_current_date))
        ut_next_date = int(next_date.timestamp())
        LOGGER.info('ut_next_date: {} '.format(ut_next_date))
        sync_metric(atx, metric, incremental_range, ut_current_date, ut_next_date)
        # if the prior sync is successful it will write the date_to_resume bookmark
        write_metrics_state(atx, metric, next_date)
        current_date = next_date

def sync_selected_streams(atx):
    selected_streams = atx.selected_stream_ids

    # last_synced_stream = atx.state.get('last_synced_stream')

    if IDS.TEAM_TABLE in selected_streams:
        sync_metrics(atx, 'team_table')

    if IDS.TAGS_TABLE in selected_streams:
        sync_metrics(atx, 'tags_table')

    if IDS.CUSTOMERS_TABLE in selected_streams:
        sync_metrics(atx, 'customers_table')

    if IDS.FIRST_RESPONSE_HISTO in selected_streams:
        sync_metrics(atx, 'first_response_histo')

    if IDS.RESOLUTION_HISTO in selected_streams:
        sync_metrics(atx, 'resolution_histo')

    if IDS.RESPONSE_HISTO in selected_streams:
        sync_metrics(atx, 'response_histo')

    if IDS.TOP_CONVERSATION_TABLE in selected_streams:
        sync_metrics(atx, 'top_conversations_table')

    if IDS.TOP_REACTION_TIME_TABLE in selected_streams:
        sync_metrics(atx, 'top_reaction_time_table')

    if IDS.TOP_REPLIES_TABLE in selected_streams:
        sync_metrics(atx, 'top_replies_table')

    # add additional analytics here
