import os

PRODDB_DBHOST=''
PRODDB_DBUSER=''
PRODDB_DBPASS=''
PRODDB_DBNAME=''

if 'PRODDB_DBHOST' in os.environ:
    PRODDB_DBHOST = os.environ['PRODDB_DBHOST']
if 'PRODDB_DBUSER' in os.environ:
    PRODDB_DBUSER = os.environ['PRODDB_DBUSER']
if 'PRODDB_DBPASS' in os.environ:
    PRODDB_DBPASS = os.environ['PRODDB_DBPASS']
if 'PRODDB_DBNAME' in os.environ:
    PRODDB_DBNAME = os.environ['PRODDB_DBNAME']

