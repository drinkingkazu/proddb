#!/usr/bin/env python
from proddb.table import table
import sys,os

if not len(sys.argv) in [2,3]:
    print 'Usage: %s PROJECT_NAME' % sys.argv[0]
    sys.exit(1)

project_name = sys.argv[1]
t=table(project_name)

if not t.exist():
    print 'Error: project does not exist:',project_name
    sys.exit(1)

if t.is_locked():
    print 'Error: project is locked!'
    sys.exit(1)

max_sequence = t.sequence()
dirs=[]
for seq in xrange(max_sequence+1):

    files = t.list(sequence=seq)
    print 'Removing',len(files),'files for sequence',seq
    for f in files:
        fname = f[4]
        d = fname[0:fname.rfind('/')]
        if not d in dirs: dirs.append(d)
        os.system('rm -f %s' % fname)

print 'Directories involved:'
for d in dirs:
    print d

for d in dirs:
    if not os.path.isdir(d): continue
    if len(os.listdir(d)) == 0:
        print 'Removing an empty dir:',d
        os.system('rm -rf %s' % d)
t.drop()

