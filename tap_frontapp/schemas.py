import os
import re

import singer
from singer import utils

class IDS(object): # pylint: disable=too-few-public-methods
    TEAM_TABLE = 'team_table'
    CUSTOMERS_HELPED_GRAPH = 'customers_helped_graph'
    FIRST_RESPONSE_GRAPH = 'first_response_graph'
    MESSAGES_RECEIVED_GRAPH = 'messages_received_graph'
    NEW_CONVERSATIONS_GRAPH = 'new_conversations_graph'
    REPLIES_SENT_GRAPH = 'replies_sent_graph'
    RESOLUTION_GRAPH = 'resolution_graph'
    RESPONSE_GRAPH = 'response_graph'

STATIC_SCHEMA_STREAM_IDS = [
    IDS.TEAM_TABLE,
    IDS.CUSTOMERS_HELPED_GRAPH,
    IDS.FIRST_RESPONSE_GRAPH,
    IDS.MESSAGES_RECEIVED_GRAPH,
    IDS.NEW_CONVERSATIONS_GRAPH,
    IDS.REPLIES_SENT_GRAPH,
    IDS.RESOLUTION_GRAPH,
    IDS.RESPONSE_GRAPH
]

PK_FIELDS = {
    IDS.TEAM_TABLE: ['analytics_date', 'analytics_range', 'teammate_v'],
    IDS.CUSTOMERS_HELPED_GRAPH: ['analytics_date', 'analytics_range', 'start', 'end'],
    IDS.FIRST_RESPONSE_GRAPH: ['analytics_date', 'analytics_range', 'start', 'end'],
    IDS.MESSAGES_RECEIVED_GRAPH: ['analytics_date', 'analytics_range', 'start', 'end'],
    IDS.NEW_CONVERSATIONS_GRAPH: ['analytics_date', 'analytics_range', 'start', 'end'],
    IDS.REPLIES_SENT_GRAPH: ['analytics_date', 'analytics_range', 'start', 'end'],
    IDS.RESOLUTION_GRAPH: ['analytics_date', 'analytics_range', 'start', 'end'],
    IDS.RESPONSE_GRAPH: ['analytics_date', 'analytics_range', 'start', 'end']
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
