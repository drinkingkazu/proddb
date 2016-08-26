#!/usr/bin/env python
import argparse,os,sys,stat,commands,socket
from proddb.table import table
from proddb.dbenv import *

parser = argparse.ArgumentParser(description='Job Submission Script (use proddb)')

parser.add_argument('-ip','--input-project', 
                    type=str, dest='inputproject',required=True,
                    help='string,  Input project name')

parser.add_argument('-op','--output-project', 
                    type=str, dest='outputproject',default='',
                    help='string,  Output project name')

parser.add_argument('-od','--output-dir',required=True, 
                    type=str, dest='outputdir',
                    help='string,  Output directory')

parser.add_argument('-ld','--log-dir', 
                    type=str, dest='logdir',required=True,
                    help='string,  Log directory')

parser.add_argument('-e','--exe',
                    type=str, dest='exe',required=True,
                    help='string, Executable')

parser.add_argument('-c','--config',  
                    type=str, dest='cfg',
                    help='string, Config file',required=True)

parser.add_argument('-as','--avoid-server',
                    type=str, dest='avoidserver',default='',
                    help='string, Server list to avoid')

args = parser.parse_args()

#
# Sanity checks
#
t=table(args.inputproject)
njobs=0
if not t.exist():
    print 'ERROR: Project does not exist:',args.inputproject
    sys.exit(1)
njobs = t.count_jobs()

if not njobs:
    print 'ERROR: No job from input project:',args.inputproject
    sys.exit(1)
if not os.path.isfile(args.exe):
    print 'ERROR: Executable does not exist:',args.exe
    sys.exit(1)
else:
    s = os.stat(args.exe)
    if not (s.st_mode & stat.S_IEXEC):
        print 'ERROR: File not an executable (permission):',args.exe
        sys.exit(1)

#
# Check avoid server option
#
server_list=[]
if args.avoidserver:
    fname=args.avoidserver
    slist = open(fname,'r').read().split()
    invalid_server = []
    valid_server = []
    for s in slist:
        try:
            valid_server.append(socket.gethostbyname(s))
            server_list.append(s)
        except Exception:
            invalid_server.append(s)
    if len(invalid_server):
        for s in invalid_server:
            print 'Invalid server:',s
        sys.exit(1)
    for x in xrange(len(valid_server)):
        print 'Avoiding a host:',server_list[x],'... IP',valid_server[x]
    print

if os.path.isdir(args.logdir) and len(os.listdir(args.logdir)):
    print 'ERROR: Log directory already exist and not empty!'
    sys.exit(1)
if os.path.isdir(args.outputdir) and len(os.listdir(args.outputdir)):
    print 'WARNING: Output directory already exist and not empty!'
    user_input=''
    while not user_input:
        sys.stdout.write('Proceed? [y/n]:')
        sys.stdout.flush()
        user_input = sys.stdin.readline().rstrip('\n')
        if not user_input.lower() in ['y','yes','n','no']:
            user_input = ''

    if user_input in ['n','no']:
        sys.exit(1)

for d in [args.logdir,args.outputdir]:
    if not os.path.isdir(d):
        try:
            os.mkdir(d)
        except OSError:
            print 'Failed to create a dir:',d
            sys.exit(1)

t=table(args.outputproject)
if t.exist():
    print 'WARNING: output project %s already exists!' % args.outputproject
    user_input=''
    while not user_input:
        sys.stdout.write('Proceed? [y/n]:')
        sys.stdout.flush()
        user_input = sys.stdin.readline().rstrip('\n')
        if not user_input.lower() in ['y','yes','n','no']:
            user_input = ''

    if user_input in ['n','no']:
        sys.exit(1)

logdir = args.logdir
outputdir = args.outputdir
cfg = args.cfg

# make sure full path is given
if not cfg.startswith('/'):
    cfg = '%s/%s' % (os.getcwd(),cfg)
if not logdir.startswith('/'):
    logdir = '%s/%s' % (os.getcwd(),logdir)
if not outputdir.startswith('/'):
    outputdir = '%s/%s' % (os.getcwd(),outputdir)

user_exe = args.exe
if not user_exe.startswith('/'):
    user_exe = os.getcwd() + '/' + args.exe
os.system('scp %s %s/' % (user_exe,logdir))
user_exe = user_exe[user_exe.rfind('/')+1:len(user_exe)]

shellexe  = ''
shellexe += '#!/usr/bin/env bash\n'
if 'LARLITE_BASEDIR' in os.environ:
    shellexe += 'source %s/config/setup.sh;\n' % os.environ['LARLITE_BASEDIR']
    shellexe += 'sleep 5;\n'
if 'LARCV_BASEDIR' in os.environ:
    shellexe += 'source %s/configure.sh;\n' % os.environ['LARCV_BASEDIR']
    shellexe += 'sleep 5;\n'
if 'PRODDB_DIR' in os.environ:
    shellexe += 'source %s/configure.sh;\n' % os.environ['PRODDB_DIR']
    shellexe += 'sleep 5;\n'
shellexe += 'mkdir %s_tmp\n' % args.outputproject
shellexe += 'cd %s_tmp\n' % args.outputproject
shellexe += 'echo recording shellenv\n'
shellexe += 'printenv >> env_$2.txt\n'
shellexe += 'scp env_$2.txt %s/\n' % logdir
shellexe += 'echo copying user executable: %s/%s\n' % (logdir,user_exe)
shellexe += 'scp %s/%s ./\n' % (logdir,user_exe)
shellexe += 'echo listing dir contents...\n'
shellexe += 'ls\n'
shellexe += 'echo executing...\n'
shellexe += './%s $1 $2 $3 $4 $5\n' % user_exe
shellexe += 'echo\n'
shellexe += 'echo listing dir contents...\n'
shellexe += 'ls\n'
shellexe += 'cd ..\n'
shellexe += 'rm -r %s_tmp\n' % args.outputproject
shellexe += 'echo done!\n'

shellexe_fname = '%s/condor_exe_%s.sh' % (logdir,args.outputproject)

cmd=''
cmd += 'Executable = %s\n' % shellexe_fname
cmd += 'Universe = vanilla\n'
cmd += 'should_transfer_files = YES\n'
cmd += 'when_to_transfer_output = ON_EXIT\n'
cmd += 'getenv = True\n'
if not server_list:
    cmd += 'Requirements = Arch == "X86_64"\n'
else:
    condition = '(Arch == "X86_64" '
    for s in server_list:
        condition += ' && Machine != "%s" ' % s

    condition += ')'
    cmd += 'Requirements = %s\n' % condition
cmd += 'Error  = %s/%s_$(PROCESS).err\n' % (logdir,args.outputproject)
cmd += 'Output = %s/%s_$(PROCESS).out\n' % (logdir,args.outputproject)
cmd += 'Log    = %s/%s_$(PROCESS).log\n' % (logdir,args.outputproject)
cmd += 'Arguments = %s $(PROCESS) %s %s %s\n' % (args.inputproject,cfg,outputdir,args.outputproject)
cmd += 'Queue %d\n' % njobs

# Copy cfg to log dir
os.system('scp %s %s' % (cfg,logdir))

# Make condor file
cmd_file = '%s/%s.condor' % (logdir,args.outputproject)
fout=open(cmd_file,'w')
fout.write(cmd)
fout.close()

# Make shell executable
fout=open(shellexe_fname,'w')
fout.write(shellexe)
fout.close()
os.system('chmod 775 %s' % shellexe_fname)

# Submit
report=commands.getoutput('condor_submit %s' % cmd_file)
jobid=report.split()[-1].rstrip('.')
if not jobid.isdigit():
    print 'Job submission failed...'
    sys.exit(1)
else:
    jobid=int(jobid)
    t=table(args.inputproject)
    t.update_job_status(status=kSTATUS_SUBMIT)
    t.register_jobid(jobid=jobid)
sys.exit(0)
