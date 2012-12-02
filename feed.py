#!/usr/bin/env python

import sys
import os
import psycopg2
import json
import urllib2
import yaml
import tempfile
import tarfile
import shutil
from config import PG_CONNECT, PACKAGE, VERSION
from changed_ids import get_changed_ids

def read_state_data(filename):
    """Read the last replication sequence and date from a YAML file and return as a tuple"""

    try:
        f = open(filename, "r")
    except IOError, e:
        sys.stderr.write("Warning: cannot open file %s: %s\n" % (filename, e))
        return (None, None)

    try:
        ytext = f.read()
    except IOError, e:
        ytext = ""

    f.close();

    data = yaml.load(ytext)
    return (int(data['replication_sequence']), data['timestamp'])
            

def save_state_data(filename, sequence, timestamp):
    """Write last replication sequence and data to a YAML file. return True/False"""

    try:
        os.makedirs(DATA_FEED_DIR)
    except os.error, e:
        if e.errno != 17: # dir exists
            sys.stderr.write("Error: cannot create data dir %s: %s\n" % (DATA_FEED_DIR, str(e)))
            sys.exit(-1)

    try:
        f = open(filename, "w")
    except IOError, e:
        sys.stderr.write("Warning: cannot open last date file %s: %s. creating new file.\n" % (filename, e))
        sys.exit(-1)

    data = { 'replication_sequence' : sequence, 'timestamp' : timestamp }
    try:
        f.write(yaml.dump(data) + "\n")
        f.close()
        return True
    except IOError, e:
        f.close();
        return False

def get_timestamp_from_replication_packet(sequence):

    packet = "ftp://ftp.musicbrainz.org/pub/musicbrainz/data/replication/replication-%d.tar.bz2" % sequence
    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', '%s/%s' % (PACKAGE, VERSION))]

    print "downloading: %s" % packet
    try:
        response = opener.open(packet)
    except urllib2.HTTPError, err:
        if err.code == 404:
            print "Replication packet not available."
            return None
        print err.code
        return None
    except urllib2.URLError, err:
        if err.reason.find("550"):
            print "Replication packet not available."
            return None
        print err.reason
        return None

    tmp = tempfile.TemporaryFile()
    shutil.copyfileobj(response, tmp)
    tmp.seek(0)

    try:
        tar_file = tarfile.open(mode="r:bz2", fileobj=tmp)
    except tarfile.ReadError:
        print "Cannot read tar file. Is the packet corrupt?"
        return None

    for f in tar_file:
        if f.name == 'TIMESTAMP':
            timestamp = tar_file.extractfile(f).read()

    return timestamp.strip()

def save_data(sequence, timestamp, data):
    '''Save the data file. If its not written for whatever reason, don't return. die.'''
    try:
        os.makedirs(OUTPUT_DIR)
    except os.error, e:
        if e.errno != 17: # dir exists
            sys.stderr.write("Error: cannot create data dir %s: %s\n" % (OUTPUT_DIR, str(e)))
            sys.exit(-1)

    filename = os.path.join(OUTPUT_DIR, "changed-ids-%d.json" % sequence)
    try:
        f = open(filename, "w")
    except os.error, e:
        sys.stderr.write("Error: cannot create data file %s: %s\n" % (filename, str(e)))
        sys.exit(-1)

    try:
        f.write(data + "\n");
    except os.error, e:
        sys.stderr.write("Error: cannot write to file: %s\n" % str(e))
        f.close();
        os.unlink(filename)
        sys.exit(-1)

    f.close()

    try:
        subprocess.check_call(["gzip", filename])
    except subprocess.CalledProcessError, e:
        sys.stderr.write("Error: cannot compress output file: %s\n" % str(e))
        f.close();
        os.unlink(filename)
        sys.exit(-1)

def generate_data_feed():

    last_date = get_last_date()
    try:
        conn = psycopg2.connect(PG_CONNECT)
    except psycopg2.OperationalError as err:
        print "Cannot connect to database: %s" % err
        sys.exit(-1)

    cur = conn.cursor()
    cur.execute("select current_replication_sequence, last_replication_date from replication_control");
    row = cur.fetchone()
    rep_seq = row[0]
    current_date = str(row[1])

    print "old date: %s\nnew date: %s\n" % (last_date, current_date)
    if current_date == last_date: 
        print "Dates are the same, no new data to process."
        return None

    if last_date:
        data = get_changed_ids(last_date, current_date)
        data = json.dumps({ 'data' : data }) #, sort_keys=True, indent=4)
        save_data(rep_seq, current_date, data)
    else:
        print "No last date available. Saving current date and doing fuck-all."

    save_last_date(current_date)
