#!/usr/bin/env python
import sys
from proddb.table import table

if len(sys.argv) > 3:
    print 'Usage: %s PROJECT_NAME [SESSION_ID]' % sys.argv[0]
    sys.exit(1)

project_name = sys.argv[1]
session_id = None
if len(sys.argv) == 3:
    session_id = int(sys.argv[2])

t=table(project_name)
if not t.exist():
    'Project does not exist:',project_name
    sys.exit(1)

format=' %-7d : %-4d : %-4s : %-4d : %s : %40s'

print
print  ' SESSION : JOB  : LOCK : STATUS : FILEPATH'
for f in t.list(session_id=session_id):
    lock='0'
    if f[2]: lock='1'
    print format % (f[0],f[1],lock,f[3],f[5],f[4])
print
