#!/usr/bin/env python
from proddb.table import table
from proddb.dbenv import *
import sys

if not len(sys.argv) in [2,3]:
    print 'Usage: %s PROJECT_NAME' % sys.argv[0]
    sys.exit(1)

project_name = sys.argv[1]
t=table(project_name)
status = kSTATUS_INIT
if len(sys.argv) == 3:
    status = int(sys.argv[2])

if not t.exist():
    print 'Error: project does not exist:',project_name
    sys.exit(1)

if t.is_locked():
    print 'Error: project is locked!'
    sys.exit(1)

t.reset_job_index(status=status)
