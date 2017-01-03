#!/usr/bin/env python
from proddb.table import table
from proddb.dbenv import *
import sys

if not len(sys.argv) in [3,4,5]:
    print 'Usage: %s PROJECT_NAME MODULAR_NUM [STATUS SEQUENCE]' % sys.argv[0]
    sys.exit(1)

project_name = sys.argv[1]
t=table(project_name)

modular = int(sys.argv[2])
status = None
if len(sys.argv)>3: status = int(sys.argv[3])
sequence = None
if len(sys.argv)>4: sequence = int(sys.argv[4])

if not t.exist():
    print 'Error: project does not exist:',project_name
    sys.exit(1)

if t.is_locked():
    print 'Error: project is locked!'
    sys.exit(1)

t.regroup_sessions(modular, status, sequence)
