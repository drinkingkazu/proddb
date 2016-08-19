import argparse,os,sys,stat
from proddb.table import table

parser = argparse.ArgumentParser(description='Job Submission Script (use proddb)')

parser.add_argument('-ip1','--input-project1', 
                    type=str, dest='inputproject1',required=True,
                    help='string,  1st Input project name')

parser.add_argument('-ip2','--input-project2', 
                    type=str, dest='inputproject2',required=True,
                    help='string,  2nd Input project name')

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

args = parser.parse_args()

#
# Sanity checks
#
t1=table(args.inputproject1)
if not t1.exist():
    print 'ERROR: Project does not exist:',args.inputproject1
    sys.exit(1)

t2=table(args.inputproject2)
if not t2.exist():
    print 'ERROR: Project does not exist:',args.inputproject2
    sys.exit(1)

if t1.count_jobs() != t2.count_jobs():
    msg = 'ERROR: two projects have different number of jobs! (%s @ %d vs. %s @ %d)' 
    msg = msg % (args.inputproject1, t1.count_jobs(), args.inputproject2, t2.count_jobs())
    print msg
    sys.exit(1)
njobs = t1.count_jobs()

t1.close()
t2.close()

if not njobs:
    print 'ERROR: No job from input projects!'
    sys.exit(1)
if not os.path.isfile(args.exe):
    print 'ERROR: Executable does not exist:',args.exe
    sys.exit(1)
else:
    s = os.stat(args.exe)
    if not (s.st_mode & stat.S_IEXEC):
        print 'ERROR: File not an executable (permission):',args.exe
        sys.exit(1)
if os.path.isdir(args.logdir) and len(os.listdir(args.logdir)):
    print 'ERROR: Log directory already exist and not empty!'
    sys.exit(1)
if os.path.isdir(args.outputdir) and len(os.listdir(args.outputdir)):
    print 'ERROR: Output directory already exist and not empty!'
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
    shellexe += 'source %s/config/setup.sh\n' % os.environ['LARLITE_BASEDIR']
if 'LARCV_BASEDIR' in os.environ:
    shellexe += 'source %s/configure.sh\n' % os.environ['LARCV_BASEDIR']
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
cmd += 'Requirements = Arch == "X86_64"\n'
cmd += 'Error  = %s/%s_$(PROCESS).err\n' % (logdir,args.outputproject)
cmd += 'Output = %s/%s_$(PROCESS).out\n' % (logdir,args.outputproject)
cmd += 'Log    = %s/%s_$(PROCESS).log\n' % (logdir,args.outputproject)
cmd += 'Arguments = %s %s %s $(PROCESS) %s %s %s\n' % (args.inputproject1,args.inputproject2,cfg,outputdir,args.outputproject)
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
os.system('condor_submit %s' % cmd_file)
