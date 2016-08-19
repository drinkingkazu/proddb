#!/usr/bin/env python
from proddb.dbconn import dbconn
from proddb.table import table
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

print
while 1:
    row = conn.cursor.fetchone()
    if not row: break
    name = row[0][7:len(row[0])]
    t=table(name)
    msg = t.describe()
    print '\033[93m%s\033[00m' % name
    print msg
    print
    
