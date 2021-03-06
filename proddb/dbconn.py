import MySQLdb
import MySQLdb.cursors
import os,sys,time

class dbconn():

    _host   = os.environ['PRODDB_DBHOST']
    _user   = os.environ['PRODDB_DBUSER']
    _passwd = os.environ['PRODDB_DBPASS']
    _dbname = os.environ['PRODDB_DBNAME']
    _conn = None
    _cursorclass = MySQLdb.cursors.Cursor
    
    def __init__(self,host='',user='',passwd='',dbname='',cursorclass=''):
        if cursorclass and cursorclass.__name__ in MySQLdb.cursors.__dict__.keys():
            sys.stderr.write('ERROR: cursor class %s not found\n' % cursorclass)
            raise Exception
        elif cursorclass and not 'Cursor' in cursorclass.__name__:
            sys.stderr.write('ERROR: not a cursor class: %s\n' % cursorclass)
            
        if host:   self._host = host
        if user:   self._user = user
        if passwd: self._passwd = passwd
        if dbname: self._dbname = dbname

        if cursorclass:
            self._cursorclass = cursorclass

    def Connect(self):
        if self._conn and self.CheckConnection(False):
            self.Close()
        try:
            self._conn = MySQLdb.connect (host=self._host,
                                         user=self._user,
                                         passwd=self._passwd,
                                         db=self._dbname,
                                         cursorclass=self._cursorclass,
                                         connect_timeout=100)
            self.cursor=self._conn.cursor()

        except MySQLdb.Error, e:
            sys.stderr.write('SQL Conn. Error %d: %s\n' % (e.args[0], e.args[1]))
            raise e
        except TypeError, e:
            sys.stderr.write('Connection failed!\n')
            raise e

    # This method checks a connection to the SQL server                                                                                   
    def CheckConnection(self,reconnect=True):
        # Check the connection to the server. Record # of trial in trial.                                                                 
        trial=1
        isConnect = False
        if self._conn:
            msg = 'Checking mySQL connection ... '
            try:
                self._conn.ping()
                msg += ' connected!'
                isConnect=True
            except MySQLdb.OperationalError, message:
                isConnect=False
                msg += ' disconnected! '
            except MySQLdb.InterfaceError, message:
                isConnect=False
                msg += ' disconnected! '
            #print msg
        #else:
        #    print "Connection instance does not exist."

        if reconnect and not isConnect:
            if self._conn:
                print "Reconnecting to mySQL server %s as %s..." % (self._host,self._user)
            while (not isConnect) and (trial < 21):
                if self._conn:
                    try:
                        self._conn.ping()
                        isConnect=True
                    except MySQLdb.OperationalError, message:
                        print 'SQL connection lost. Reconnecting...trial %d' % trial
                    except MySQLdb.InterfaceError, message:
                        print 'SQL connection lost. Reconnecting...trial %d' % trial
                # If the connection is lost, re-connect                                                                                   
                if not isConnect:
                    now=time.gmtime()
                    now="%d-%d-%d %d:%d:%d" % (now[0],now[1],now[2],now[3],now[4],now[5])
                    if trial>1 :
                        print "MySQL Reconnection attempt in 60 seconds..."
                        time.sleep(60)
                        print "Reconnecting to mySQL server... %s" % now
                    self.Connect()
                else:
                    break
                trial += 1

        return isConnect

    # Execute SQL command                                                                                                                 
    def ExecSQL(self,cmd):
        fBool=True
        # Check connection                                                                                                                
        if not self.CheckConnection():
            sys.stderr.write('ERROR: mySQL connection cannot be established...\n')
            fBool=False
        else:
            # Try the command. If not success, return false.                                                                              
            try:
                #print "mySQL Query: %s" % cmd
                self.cursor.execute(cmd)
                fBool=True
            except MySQLdb.Error,e:
                print e
                sys.stderr.write('Error while executing: %s\n' % cmd)
                fBool=False
                return fBool

    # Check table existence                                                                                                               
    def ExistTable(self,tablename):
        #if tablename in DCProdDBIHandler._existTables:
        #    return True
        temp='SHOW TABLES LIKE \'%s\'' % tablename
        self.ExecSQL(temp)
        nrow = self.cursor.rowcount
        if nrow <1:
            return False
        else:
            #DCProdDBIHandler._existTables.append(tablename)
            return True

    # Close connection                                                                                                                    
    def Close(self):
        fBool=self.CheckConnection(False)
        if fBool:
            if not self.cursor:
                sys.stderr.write('Connection found but not cursor. Reason Not Known!!!\n')
            else:
                #print "Closing mySQL connection..."
                self.cursor.close()
            self._conn.commit()
            self._conn.close()
        else:
            print "Requested to close mySQL connection: already closed!"
        return fBool

if __name__ == '__main__':
    
    conn = dbconn()
    conn.Connect()
    conn.Close()
