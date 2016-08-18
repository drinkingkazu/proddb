import sys
from pyproddb.table import table

if len(sys.argv) > 3:
    print 'Usage: %s PROJECT_NAME [SESSION_ID]' % sys.argv[0]
    sys.exit(1)

project_name = sys.argv[0]
session_id = None
if len(sys.argv) == 3:
    session_id = int(sys.argv[2])

t=table(project_name)
if not t.exist():
    'Project does not exist:',project_name
    sys.exit(1)

for f in t.list(session_id=session_id):
    print f


