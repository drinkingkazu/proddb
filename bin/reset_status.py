#!/usr/bin/env python
import argparse,os,sys,stat
from proddb.table import table

parser = argparse.ArgumentParser(description='proddb status reset script')

parser.add_argument('-p','--project', 
                    type=str, dest='project',required=True,
                    help='string, Project name')

parser.add_argument('-ts','--target-status',default=None,
                    type=int, dest='cstatus',
                    help='int, target status value')

parser.add_argument('-ns','--new-status',default=None,
                    type=int, dest='nstatus',
                    help='int, new status value')

parser.add_argument('-ji','--job-index',default=None, 
                    type=int, dest='jobindex',
                    help='int, target job index')

parser.add_argument('-ss','--session',default=None,
                    type=int, dest='session',
                    help='int, target session')


    def update_status(self,status,job_index=None,session_id=None,sequence=None):

args = parser.parse_args()

#
# Sanity checks
#
t=table(args.inputproject)
if not t.exist():
    print 'ERROR: Project does not exist:',args.inputproject
    sys.exit(1)

t.update_status(status=int(args.nstatus),target_status=int(args.cstatus),job_index=int(args.jobindex),session_id=int(args.session))
