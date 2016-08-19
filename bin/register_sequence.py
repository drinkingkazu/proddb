import sys,os

if not len(sys.argv) == 3:
    print 'Usage: %s PROJECT_NAME FILE_LIST' % sys.argv[0]
    sys.exit(1)

flist=[]
for l in open(sys.argv[2],'r').read().split('\n'):
    words=l.split()
    if not len(words) == 2:
        print 'Format error: must be 2 words per line (session id, file path)'
        raise Exception
    if not words[0].isdigit():
        print 'Format error: must be 2 words per line (session id, file path)'    
        raise Exception
    if not os.path.isfile(words[1]):
        print 'File not found:',words[1]
        raise Exception
    flist.append((int(words[0]),words[1]))

project_name = sys.argv[1]

from proddb.table import table

t=table(project_name)

if t.exist():
    print 'Project',project_name,'already exist.'
    user_input=''
    while not user_input.lower() in ['y','n','yes','no']:
        sys.stdout.write('Would you like to continue? [y/n]:')
        sys.stdout.flush()
        user_input = sys.stdin.read().strip('\n')

    if not user_input in ['y','yes']:
        sys.exit(0)

seq = t.sequence()
if seq: seq += 1
    
for info in flist:
    t.fill(filepath=info[1],session_id=info[0],status=0,sequence=seq)



