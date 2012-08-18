#!/usr/bin/env python

import sys
import psycopg2
import json
from config import PG_CONNECT
from changed_ids import get_changed_ids

def generate_data_feed(start, end):
    try:
        conn = psycopg2.connect(PG_CONNECT)
    except psycopg2.OperationalError as err:
        print "Cannot connect to database: %s" % err
        sys.exit(-1)

    data = get_changed_ids(start, end)
    print json.dumps({ 'data' : data }) #, sort_keys=True, indent=4)

if len(sys.argv) < 3:
    print "Usage: %s <start> <end>" % sys.argv[0]
    print
    print "  <start> - timestamp of start time in YYYY-MM-DD hh:mm format"
    print "  <end>   - timestamp of end time in YYYY-MM-DD hh:mm format"
    sys.exit(-1)

generate_data_feed(sys.argv[1], sys.argv[2])
