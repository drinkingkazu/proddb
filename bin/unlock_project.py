from proddb.table import table
import sys

if not len(sys.argv) == 2:
    print 'Usage: %s PROJECT_NAME' % sys.argv[0]
    sys.exit(1)

project_name = sys.argv[1]
t=table(project_name)

if not t.exist():
    print 'Error: project does not exist:',project_name
    sys.exit(1)

t.unlock()
