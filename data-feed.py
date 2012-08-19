#!/usr/bin/env python

import sys
import os
import psycopg2
import json
import subprocess
from config import PG_CONNECT, DATA_FEED_DIR
from changed_ids import get_changed_ids

LAST_UPDATED_FILE = os.path.join(DATA_FEED_DIR, "last_updated")
OUTPUT_DIR = os.path.join(DATA_FEED_DIR, "data")

def get_last_date():
    """Get the last date this program ran from the last_upated file. Could return blank string if no file or bad data is found."""

    try:
        f = open(LAST_UPDATED_FILE, "r")
    except IOError, e:
        sys.stderr.write("Warning: cannot open file %s: %s\n" % (LAST_UPDATED_FILE, e))
        return ""

    try:
        line = f.readline()
    except IOError, e:
        line = ""

    f.close();
    return line.strip()

def save_last_date(line):
    """Write the last date this program ran the LAST_UPDATED_FILE. Returns t/f."""

    try:
        os.makedirs(DATA_FEED_DIR)
    except os.error, e:
        if e.errno != 17: # dir exists
            sys.stderr.write("Error: cannot create data dir %s: %s\n" % (DATA_FEED_DIR, str(e)))
            sys.exit(-1)

    try:
        f = open(LAST_UPDATED_FILE, "w")
    except IOError, e:
        sys.stderr.write("Warning: cannot open last date file %s: %s. creating new file.\n" % (LAST_UPDATED_FILE, e))
        sys.exit(-1)

    try:
        f.write(str(line) + "\n")
        f.close()
        return True
    except IOError, e:
        f.close();
        return False

def save_data(sequence, date, data):
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
        subprocess.check_call(["bzip2", filename])
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

generate_data_feed()
