import sys,os
from pyproddb.table import table

if not len(sys.argv) <4:
    print 'USAGE: %s PROJECT_NAME SESSION_ID [FILE1 FILE2 ...]' % sys.argv[0]
    sys.exit(1)
if not sys.argv[2].isdigit():
    print 'Session ID must be integer...'
    sys.exit(1)

t=table(sys.argv[1])
if not t.exist():
    t.create()

session = int(sys.argv[2])

files=sys.argv[3:]
for f in files:
    if not os.path.isfile(f):
        print 'File not found:',f
        sys.exit(1)
    
t.register_session(file_v = files, check=False)




