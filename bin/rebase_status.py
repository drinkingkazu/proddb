#!/usr/bin/env python
import argparse,os,sys,stat
from proddb.table import table

parser = argparse.ArgumentParser(description='proddb status reset script')

parser.add_argument('-p','--project', 
                    type=str, dest='project',required=True,
                    help='string, project name')

parser.add_argument('-s','--status',required=True,
                    type=int, dest='status',
                    help='int, status value')

parser.add_argument('-tp','--target-project',required=True,
                    type=str, dest='target',
                    help='string, target project')

args = parser.parse_args()

#
# Sanity checks
#
t1=table(args.project)
if not t1.exist():
    print 'ERROR: Project does not exist:',args.project
    sys.exit(1)

t2=table(args.target)
if not t1.exist():
    print 'ERROR: Project does not exist:',args.target
    sys.exit(1)

sessions1=t1.unique_sessions()
sessions2=t2.unique_sessions()

for s in sessions2:
    
    if not s in sessions1:

        print 'ERROR: found a session in target project that does not exist in the input!'
        sys.exit(1)

for s in sessions1:

    if not s in sessions2:

        t1.update_status(status=args.status,session_id=s)

