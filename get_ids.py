#!/usr/bin/env python

import sys
import psycopg2;
from config import PG_CONNECT

def get_artist_gids(conn, start, end):

    arguments = []
    queries = [
        "select gid from %schema%.artist where last_updated > %s AND last_updated <= %s",
        "select gid from %schema%.artist_alias aa join %schema%.artist a on aa.artist = a.id where aa.last_updated > %s AND aa.last_updated < %s", 
        "select gid from %schema%.artist_annotation aa join %schema%.artist a on aa.artist = a.id join %schema%.annotation an on aa.annotation = an.id where an.created > %s AND an.created < %s",
        "select gid from %schema%.artist_ipi ai join %schema%.artist a on ai.artist = a.id where ai.created > %s AND ai.created < %s",
        "select gid from %schema%.artist_tag at join %schema%.artist a on at.artist = a.id where at.last_updated > %s AND at.last_updated < %s",
        "select gid from %schema%.artist_credit ac join %schema%.artist_credit_name acn on acn.artist_credit = ac.id join %schema%.artist a on acn.artist = a.id where ac.created > %s AND ac.created < %s"
    ]

    for q in queries: arguments.extend((start, end))

    query = "SELECT DISTINCT(gid) FROM (\n";
    query += '\n  UNION\n'.join(queries)
    query += '\n) as gid'
    query = query.replace("%schema%", "musicbrainz")

    cur = conn.cursor()
    cur.execute(query, arguments)
    rows = cur.fetchall()
    for row in rows:
        print row[0]

try:
    conn = psycopg2.connect(PG_CONNECT)
except psycopg2.OperationalError as err:
    print "Cannot connect to database: %s" % err
    sys.exit(-1)

get_artist_gids(conn, sys.argv[1], sys.argv[2])
