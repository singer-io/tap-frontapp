# pylint: disable=E1101

import time
import datetime

import pendulum
import requests
import singer
from singer.bookmarks import write_bookmark
from ratelimit import limits, sleep_and_retry, RateLimitException
from backoff import on_exception, expo, constant

from .http import MetricsRateLimitException

LOGGER = singer.get_logger()

MAX_METRIC_JOB_TIME = 1800
METRIC_JOB_POLL_SLEEP = 3

FRONT_REPORT_API_AVAILABLE_METRICS = [
    "avg_first_response_time",
    "avg_handle_time",
    "avg_response_time",
    "avg_sla_breach_time",
    "avg_total_reply_time",
    "new_segments_count",
    "num_active_segments_full",
    "num_archived_segments",
    "num_archived_segments_with_reply",
    "num_csat_survey_response",
    "num_messages_received",
    "num_messages_sent",
    "num_sla_breach",
    "pct_csat_survey_satisfaction",
    "pct_tagged_conversations",
    "num_open_segments_start",
    "num_closed_segments",
    "num_open_segments_end",
    "num_workload_segments"
]

METRIC_API_PATH = {
    'accounts_table': '/accounts',
    'channels_table': '/channels',
    'inboxes_table': '/inboxes',
    'tags_table': '/tags',
    'teammates_table': '/teammates',
    'teams_table': '/teams',
}

METRIC_API_FILTER_NAME = {
    'accounts_table': 'account_ids',
    'channels_table': 'channel_ids',
    'inboxes_table': 'inbox_ids',
    'tags_table': 'tag_ids',
    'teammates_table': 'teammate_ids',
    'teams_table': 'team_ids',
}

METRIC_API_DESCRIPTION_KEY = {
    'accounts_table': 'name',
    'channels_table': 'name',
    'inboxes_table': 'name',
    'tags_table': 'name',
    'teammates_table': 'email',
    'teams_table': 'name',
}


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
@limits(calls=1, period=61)  # 60 seconds needed to be padded by 1 second to work
def get_report_metrics(atx, report_url):
    return atx.client.get_report_metrics(report_url)


def create_report(atx, start_date, end_date, filters):
    params = {
        'start': start_date,
        'end': end_date,
        'metrics': FRONT_REPORT_API_AVAILABLE_METRICS,
        'filters': filters,
    }
    try:
        report_url = atx.client.create_report('/analytics/reports', data=params)
        return report_url
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == requests.codes.bad_request:
            LOGGER.info(f'[Skipping] Could not generate report for params {params}: {e}.')
        else:
            raise e


def sync_metric(atx, metric_name, start_date, end_date):
    for metric in atx.client.list_metrics(path=METRIC_API_PATH[metric_name]):
        metric_id = metric['id']
        metric_description = metric[METRIC_API_DESCRIPTION_KEY[metric_name]]
        report_url = create_report(atx, start_date, end_date,
                                   filters={METRIC_API_FILTER_NAME[metric_name]: [metric_id]})
        if not report_url:
            continue

        with singer.metrics.job_timer('daily_aggregated_metric'):
            start = time.monotonic()
            start_date_formatted = datetime.datetime.utcfromtimestamp(start_date).strftime('%Y-%m-%d')
            # we've really moved this functionality to the request in the http script
            # so we don't expect that this will actually have to run mult times
            while True:
                if (time.monotonic() - start) >= MAX_METRIC_JOB_TIME:
                    raise Exception('Metric job timeout ({} secs)'.format(
                        MAX_METRIC_JOB_TIME))

                LOGGER.info('Metrics query - report_url: {} start_date: {} end_date: {} {}: {} ({})'.format(
                    report_url,
                    start_date,
                    end_date,
                    metric_name,
                    metric_id,
                    metric_description
                ))
                report_metrics = get_report_metrics(atx, report_url)
                if report_metrics != '':
                    break
                else:
                    time.sleep(METRIC_JOB_POLL_SLEEP)

        record = {
            "report_id": report_url.split('/')[-1],
            "analytics_date": start_date_formatted,
            "analytics_range": 'daily',
            "metric_id": metric_id,
            "metric_description": metric_description,
            **{report_metric["id"]: report_metric["value"] for report_metric in report_metrics}
        }
        write_records(metric_name, [record])


def write_metrics_state(atx, metric, date_to_resume):
    write_bookmark(atx.state, metric, 'date_to_resume', date_to_resume.to_datetime_string())
    atx.write_state()


def sync_metrics(atx, metric_name):
    bookmark = atx.state.get('bookmarks', {}).get(metric_name, {})
    LOGGER.info('metric: {} '.format(metric_name))

    # start_date is defaulted in the config file 2018-01-01
    # if there's no default date and it gets set to now, then start_date will have to be
    #   set to the prior business day before we can use it.
    now = datetime.datetime.now()
    s_d = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = pendulum.parse(atx.config.get('start_date', s_d + datetime.timedelta(days=-1, hours=0)))
    LOGGER.info('start_date: {} '.format(start_date))

    # end date is not usually specified in the config file by default so end_date is now.
    # if end date is now, we will have to truncate them
    # to the nearest day before we can use it.
    e_d = now.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
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
        next_date = current_date + datetime.timedelta(days=1, hours=0)

        ut_current_date = int(current_date.timestamp())
        LOGGER.info('ut_current_date: {} '.format(ut_current_date))
        ut_next_date = int(next_date.timestamp())
        LOGGER.info('ut_next_date: {} '.format(ut_next_date))
        sync_metric(atx, metric_name, ut_current_date, ut_next_date)
        # if the prior sync is successful it will write the date_to_resume bookmark
        write_metrics_state(atx, metric_name, next_date)
        current_date = next_date


def sync_selected_streams(atx):
    for selected_stream in atx.selected_stream_ids:
        sync_metrics(atx, selected_stream)
