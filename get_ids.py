#!/usr/bin/env python

import sys
import psycopg2;
from config import PG_CONNECT

# TODO: an AR type changes

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

label_queries = [
    "select gid from %schema%.l_artist_label lal join %schema%.label l on lal.entity1 = l.id where lal.last_updated >= %s and lal.last_updated < %s",
    "select gid from %schema%.l_label_label lll join %schema%.label l on lll.entity1 = l.id where lll.last_updated >= %s and lll.last_updated < %s",
    "select gid from %schema%.l_label_label lll join %schema%.label l on lll.entity0 = l.id where lll.last_updated >= %s and lll.last_updated < %s",
    "select gid from %schema%.l_label_recording llr join %schema%.label l on llr.entity0 = l.id where llr.last_updated >= %s and llr.last_updated < %s",
    "select gid from %schema%.l_label_release llr join %schema%.label l on llr.entity0 = l.id where llr.last_updated >= %s and llr.last_updated < %s",
    "select gid from %schema%.l_label_release_group llr join %schema%.label l on llr.entity0 = l.id where llr.last_updated >= %s and llr.last_updated < %s",
    "select gid from %schema%.l_label_url llu join %schema%.label l on llu.entity0 = l.id where llu.last_updated >= %s and llu.last_updated < %s",
    "select gid from %schema%.l_label_work llw join %schema%.label l on llw.entity0 = l.id where llw.last_updated >= %s and llw.last_updated < %s",
    "select gid from %schema%.label where last_updated >= %s AND last_updated < %s",
    "select gid from %schema%.label_alias la join %schema%.label l on la.label = l.id where la.last_updated >= %s AND la.last_updated < %s", 
    "select gid from %schema%.label_annotation la join %schema%.label l on la.label = l.id join %schema%.annotation an on la.annotation = an.id where an.created >= %s AND an.created < %s",
    "select gid from %schema%.label_ipi li join %schema%.label l on li.label = l.id where li.created >= %s AND li.created < %s",
    "select gid from %schema%.label_tag lt join %schema%.label l on lt.label = l.id where lt.last_updated >= %s AND lt.last_updated < %s",
    "select l.gid from label_gid_redirect lgr join label l on lgr.new_id = l.id where created >= %s and created < %s",
    "select gid from label_gid_redirect where created >= %s and created < %s",
    "select gid from %schema%.release_label rl join %schema%.label l on rl.label = l.id where rl.last_updated >= %s AND rl.last_updated < %s",
]

recording_queries = [
    "select gid from %schema%.isrc i join %schema%.recording r on i.recording = r.id where i.created >= %s AND i.created < %s",
    "select gid from %schema%.l_artist_recording lar join %schema%.recording r on lar.entity1 = r.id where lar.last_updated >= %s and lar.last_updated < %s",
    "select gid from %schema%.l_label_recording llr join %schema%.label l on llr.entity1 = l.id where llr.last_updated >= %s and llr.last_updated < %s",
    "select gid from %schema%.l_recording_recording rrr join %schema%.recording r on rrr.entity1 = r.id where rrr.last_updated >= %s and rrr.last_updated < %s",
    "select gid from %schema%.l_recording_recording rrr join %schema%.recording r on rrr.entity0 = r.id where rrr.last_updated >= %s and rrr.last_updated < %s",
    "select gid from %schema%.l_recording_release lrr join %schema%.recording r on lrr.entity0 = r.id where lrr.last_updated >= %s and lrr.last_updated < %s",
    "select gid from %schema%.l_recording_release_group lrr join %schema%.recording r on lrr.entity0 = r.id where lrr.last_updated >= %s and lrr.last_updated < %s",
    "select gid from %schema%.l_recording_url lru join %schema%.recording r on lru.entity0 = r.id where lru.last_updated >= %s and lru.last_updated < %s",
    "select gid from %schema%.l_recording_work lrw join %schema%.recording r on lrw.entity0 = r.id where lrw.last_updated >= %s and lrw.last_updated < %s",
    "select gid from %schema%.recording where last_updated >= %s AND last_updated < %s",
    "select gid from %schema%.recording_annotation ra join %schema%.recording r on ra.recording = r.id join %schema%.annotation an on ra.annotation = an.id where an.created >= %s AND an.created < %s",
    "select r.gid from recording_gid_redirect rgr join recording r on rgr.new_id = r.id where created >= %s and created < %s",
    "select gid from recording_gid_redirect where created >= %s and created < %s",
    "select gid from %schema%.recording_tag rt join %schema%.recording r on rt.recording = r.id where rt.last_updated >= %s AND rt.last_updated < %s",
    "select gid from %schema%.track t join %schema%.recording r on t.recording = r.id where t.last_updated >= %s AND t.last_updated < %s"
]

release_group_queries = [
    "select gid from %schema%.l_artist_release_group lar join %schema%.release_group r on lar.entity1 = r.id where lar.last_updated >= %s and lar.last_updated < %s",
    "select gid from %schema%.l_label_release_group llr join %schema%.label l on llr.entity1 = l.id where llr.last_updated >= %s and llr.last_updated < %s",
    "select gid from %schema%.l_recording_release_group lrr join %schema%.release_group r on lrr.entity1 = r.id where lrr.last_updated >= %s and lrr.last_updated < %s",
    "select gid from %schema%.l_release_release_group lrr join %schema%.release_group r on lrr.entity1 = r.id where lrr.last_updated >= %s and lrr.last_updated < %s",
    "select gid from %schema%.l_release_group_release_group rrr join %schema%.release_group r on rrr.entity0 = r.id where rrr.last_updated >= %s and rrr.last_updated < %s",
    "select gid from %schema%.l_release_group_release_group rrr join %schema%.release_group r on rrr.entity1 = r.id where rrr.last_updated >= %s and rrr.last_updated < %s",
    "select gid from %schema%.l_release_group_url lru join %schema%.release_group r on lru.entity0 = r.id where lru.last_updated >= %s and lru.last_updated < %s",
    "select gid from %schema%.l_release_group_work lrw join %schema%.release_group r on lrw.entity0 = r.id where lrw.last_updated >= %s and lrw.last_updated < %s",
    "select rg.gid from %schema%.release r join %schema%.release_group rg on r.release_group = rg.id where r.last_updated >= %s AND r.last_updated < %s",
    "select gid from %schema%.release_group where last_updated >= %s AND last_updated < %s",
    "select gid from %schema%.release_group_annotation ra join %schema%.release_group r on ra.release_group = r.id join %schema%.annotation an on ra.annotation = an.id where an.created >= %s AND an.created < %s",
    "select r.gid from release_group_gid_redirect rgr join release_group r on rgr.new_id = r.id where created >= %s and created < %s",
    "select gid from release_group_gid_redirect where created >= %s and created < %s",
    "select gid from %schema%.release_group_tag rt join %schema%.release_group r on rt.release_group = r.id where rt.last_updated >= %s AND rt.last_updated < %s"
]

def debug(conn, queries, start, end):
    cur = conn.cursor()
    for q in queries: 
        q = q.replace("%schema%", "musicbrainz")
        cur.execute(q + " order by gid", (start, end))
        rows = cur.fetchall()
        if cur.rowcount:
            print "%s:" % q
            for row in rows:
                print "   " + row[0]

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

get_gids(conn, release_group_queries, sys.argv[1], sys.argv[2])
