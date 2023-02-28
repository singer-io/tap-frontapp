import os
import re

import singer
from singer import utils


class IDS(object):  # pylint: disable=too-few-public-methods
    ACCOUNTS_TABLE = 'accounts_table'
    CHANNELS_TABLE = 'channels_table'
    INBOXES_TABLE = 'inboxes_table'
    TAGS_TABLE = 'tags_table'
    TEAMMATES_TABLE = 'teammates_table'
    TEAMS_TABLE = 'teams_table'


STATIC_SCHEMA_STREAM_IDS = [
    IDS.ACCOUNTS_TABLE,
    IDS.CHANNELS_TABLE,
    IDS.INBOXES_TABLE,
    IDS.TAGS_TABLE,
    IDS.TEAMMATES_TABLE,
    IDS.TEAMS_TABLE,
]

PK_FIELDS = {
    IDS.ACCOUNTS_TABLE: ['analytics_date', 'analytics_range', 'report_id', 'metric_id'],
    IDS.CHANNELS_TABLE: ['analytics_date', 'analytics_range', 'report_id', 'metric_id'],
    IDS.INBOXES_TABLE: ['analytics_date', 'analytics_range', 'report_id', 'metric_id'],
    IDS.TAGS_TABLE: ['analytics_date', 'analytics_range', 'report_id', 'metric_id'],
    IDS.TEAMMATES_TABLE: ['analytics_date', 'analytics_range', 'report_id', 'metric_id'],
    IDS.TEAMS_TABLE: ['analytics_date', 'analytics_range', 'report_id', 'metric_id'],
}


def normalize_fieldname(fieldname):
    fieldname = fieldname.lower()
    fieldname = re.sub(r'[\s\-]', '_', fieldname)
    return re.sub(r'[^a-z0-9_]', '', fieldname)


# the problem with the schema we see coming from team_table is that it's a little inconsistent:
# {"t":"str","v":"All","p":"All"},{"t":"num","v":306,"p":465},{"t":"num","v":2.65,"p":2.39},...
# ,[{"t":"teammate","v":"Andrew","url":"/api/1/companies/theguild_co/team/andrew","id":253419},...
# so we see that the type = teammate is different when it covers All team members
# also we see the schema where type = num or dur is actually a triplet of type, value, and previous
# so it looks like we need to hardcode those anomalies into this file

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = 'schemas/{}.json'.format(tap_stream_id)
    # print("schema path=",path)
    return utils.load_json(get_abs_path(path))


def load_and_write_schema(tap_stream_id):
    schema = load_schema(tap_stream_id)
    singer.write_schema(tap_stream_id, schema, PK_FIELDS[tap_stream_id])
