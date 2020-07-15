import os
import re

import singer
from singer import utils

class IDS(object): # pylint: disable=too-few-public-methods
    TEAM_TABLE = 'team_table'
    TAGS_TABLE = 'tags_table'
    CUSTOMERS_TABLE = 'customers_table'
    FIRST_RESPONSE_HISTO = 'first_response_histo'
    RESOLUTION_HISTO = 'resolution_histo'
    RESPONSE_HISTO = 'response_histo'
    TOP_CONVERSATION_TABLE = 'top_conversations_table'
    TOP_REACTION_TIME_TABLE = 'top_reaction_time_table'
    TOP_REPLIES_TABLE = 'top_replies_table'

STATIC_SCHEMA_STREAM_IDS = [
    IDS.TEAM_TABLE,
    IDS.TAGS_TABLE,
    IDS.CUSTOMERS_TABLE,
    IDS.FIRST_RESPONSE_HISTO,
    IDS.RESOLUTION_HISTO,
    IDS.RESPONSE_HISTO,
    IDS.TOP_CONVERSATION_TABLE,
    IDS.TOP_REACTION_TIME_TABLE,
    IDS.TOP_REPLIES_TABLE
]

PK_FIELDS = {
    IDS.TEAM_TABLE: ['analytics_date', 'analytics_range', 'teammate_v'],
    IDS.TAGS_TABLE: ['analytics_date', 'analytics_range', 'tag_v'],
    IDS.CUSTOMERS_TABLE: ['analytics_date', 'analytics_range', 'resource_t', 'resource_v'],
    IDS.FIRST_RESPONSE_HISTO: ['analytics_date', 'analytics_range', 'time_v'],
    IDS.RESOLUTION_HISTO: ['analytics_date', 'analytics_range', 'time_v'],
    IDS.RESPONSE_HISTO: ['analytics_date', 'analytics_range', 'time_v'],
    IDS.TOP_CONVERSATION_TABLE: ['analytics_date', 'analytics_range', 'teammate_v'],
    IDS.TOP_REACTION_TIME_TABLE: ['analytics_date', 'analytics_range', 'teammate_v'],
    IDS.TOP_REPLIES_TABLE: ['analytics_date', 'analytics_range', 'teammate_v']
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
    #print("schema path=",path)
    return utils.load_json(get_abs_path(path))

def load_and_write_schema(tap_stream_id):
    schema = load_schema(tap_stream_id)
    singer.write_schema(tap_stream_id, schema, PK_FIELDS[tap_stream_id])
