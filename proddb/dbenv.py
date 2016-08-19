import os

DLPROD_DBHOST=''
DLPROD_DBUSER=''
DLPROD_DBPASS=''
DLPROD_DBNAME=''

if 'DLPROD_DBHOST' in os.environ:
    DLPROD_DBHOST = os.environ['DLPROD_DBHOST']
if 'DLPROD_DBUSER' in os.environ:
    DLPROD_DBUSER = os.environ['DLPROD_DBUSER']
if 'DLPROD_DBPASS' in os.environ:
    DLPROD_DBPASS = os.environ['DLPROD_DBPASS']
if 'DLPROD_DBNAME' in os.environ:
    DLPROD_DBNAME = os.environ['DLPROD_DBNAME']

