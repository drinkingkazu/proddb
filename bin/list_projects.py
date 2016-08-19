#!/usr/bin/env python
from proddb.dbconn import dbconn

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

while 1:
    row = conn.cursor.fetchone()
    if not row: break
    name = row[0]
    print name[7:len(name)]
