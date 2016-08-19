#!/usr/bin/env python
import sys
from proddb.table import table

if not len(sys.argv) == 2:
    print 'Usage: %s PROJECT_NAME ' % sys.argv[0]
    sys.exit(1)

project_name = sys.argv[1]

t=table(project_name)
if not t.exist():
    'Project does not exist:',project_name
    sys.exit(1)

format=' %-4d : %-4d   : %-10s : %s'

print
print  ' JOB  : STATUS : JOB-ID     : TIME '
for f in t.list_jobs():
    print format % (f[0],f[1],f[2],f[3])
print
