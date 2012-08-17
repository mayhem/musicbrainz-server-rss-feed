#!/usr/bin/env python

import sys
import psycopg2;
from config import PG_CONNECT

artist_queries = [
    "select gid from %schema%.artist where last_updated >= %s AND last_updated < %s",
    "select gid from %schema%.artist_alias aa join %schema%.artist a on aa.artist = a.id where aa.last_updated >= %s AND aa.last_updated < %s", 
    "select gid from %schema%.artist_annotation aa join %schema%.artist a on aa.artist = a.id join %schema%.annotation an on aa.annotation = an.id where an.created >= %s AND an.created < %s",
    "select gid from %schema%.artist_ipi ai join %schema%.artist a on ai.artist = a.id where ai.created >= %s AND ai.created < %s",
    "select gid from %schema%.artist_tag at join %schema%.artist a on at.artist = a.id where at.last_updated >= %s AND at.last_updated < %s",
    "select gid from %schema%.artist_credit ac join %schema%.artist_credit_name acn on acn.artist_credit = ac.id join %schema%.artist a on acn.artist = a.id where ac.created >= %s AND ac.created < %s",
    "select a.gid from artist_gid_redirect agr join artist a on agr.new_id = a.id where created >= %s and created < %s",
    "select gid from artist_gid_redirect where created >= %s and created < %s",
    "select gid from %schema%.l_artist_artist laa join %schema%.artist a on laa.entity1 = a.id where laa.last_updated >= %s and laa.last_updated < %s",
    "select gid from %schema%.l_artist_artist laa join %schema%.artist a on laa.entity0 = a.id where laa.last_updated >= %s and laa.last_updated < %s",
    "select gid from %schema%.l_artist_label lal join %schema%.artist a on lal.entity0 = a.id where lal.last_updated >= %s and lal.last_updated < %s",
    "select gid from %schema%.l_artist_recording lar join %schema%.artist a on lar.entity0 = a.id where lar.last_updated >= %s and lar.last_updated < %s",
    "select gid from %schema%.l_artist_release lar join %schema%.artist a on lar.entity0 = a.id where lar.last_updated >= %s and lar.last_updated < %s",
    "select gid from %schema%.l_artist_release_group lar join %schema%.artist a on lar.entity0 = a.id where lar.last_updated >= %s and lar.last_updated < %s",
    "select gid from %schema%.l_artist_url lau join %schema%.artist a on lau.entity0 = a.id where lau.last_updated >= %s and lau.last_updated < %s",
    "select gid from %schema%.l_artist_work law join %schema%.artist a on law.entity0 = a.id where law.last_updated >= %s and law.last_updated < %s"
]

def get_gids(conn, queries, start, end):

    arguments = []

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

get_gids(conn, artist_queries, sys.argv[1], sys.argv[2])
