#!/usr/bin/env python
from proddb.dbconn import dbconn
from proddb.table import table
from proddb.dbenv import *
import commands

import sys

if len(sys.argv) > 2:
    print 'Usage: %s [KEYWORD]' % sys.argv[0]
    sys.exit(1)

conn=dbconn()

query='SHOW TABLES LIKE \'filedb_%'
if len(sys.argv) == 2:
    query += sys.argv[1] + '%'
query += '\';'
conn.ExecSQL(query)

projects = []
while 1:
    row = conn.cursor.fetchone()
    if not row: break
    projects.append(row[0][7:len(row[0])])

condor_q = commands.getoutput('condor_q')

jobmap={}
for l in condor_q.split('\n'):
    words=l.split()
    if not len(words): 
        continue

    words = words[0].split('.')
    if not len(words) == 2 or not words[0].isdigit() or not words[1].isdigit(): 
        continue
    
    jobid=int(words[0])
    seq=int(words[1])
    
    if not jobid in jobmap: jobmap[jobid]=[]
    jobmap[jobid].append(seq)

for p in projects:

    t=table(p)
    jobinfo_db = t.job_info()

    for jobid_key,status_v in jobinfo_db.iteritems():

        if not jobid_key.isdigit(): continue
        jobid = int(jobid_key)

        failed_seq=[]
        if not jobid in jobmap:
            failed_seq += status_v[kSTATUS_SUBMIT]
            failed_seq += status_v[kSTATUS_RUNNING]
        else:
            for seq in status_v[kSTATUS_SUBMIT] + status_v[kSTATUS_RUNNING]:
                if seq in jobmap[jobid]: continue
                failed_seq.append(seq)

        if len(failed_seq):
            for seq in failed_seq:
                t.update_status(status=kSTATUS_FAILED,job_index=seq,extra_data=str(jobid_key))

