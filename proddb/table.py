from dbconn import dbconn
import sys

class table():

    _table_prefix = 'filedb_'

    def __init__(self,name,conn=None):
        if not conn: self._conn = dbconn()
        else: self._conn = conn
        if not name or not type(name) == type(str()):
            sys.stderr.write('ERROR: must provide non-empty string as a table name!\n')
            raise Exception
        self._name = name
        self._sequence = None

    def table_name(self):
        return self.__class__._table_prefix + self._name

    def close(self):
        self._conn.Close()

    def drop(self):

        query = 'DROP TABLE %s;' % self.table_name()
        self._conn.ExecSQL(query)
        
    def create(self):

        if self.exist(): return

        query  = 'CREATE TABLE ' + self.table_name()
        query += '( '
        query += 'id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,'
        query += 'time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,'
        query += 'sequence   INT NOT NULL,'
        query += 'session_id SMALLINT NOT NULL,'
        query += 'status     SMALLINT,'
        query += 'job_index  SMALLINT,'
        query += 'joblock    BOOLEAN,'
        query += 'filepath   VARCHAR(200)'
        query += ');'

        self._conn.ExecSQL(query)

    def exist(self):
        return self._conn.ExistTable(self.table_name())

    def sequence(self):

        if self._sequence is not None:
            return self._sequence
        
        query = 'SELECT SEQUENCE FROM %s ORDER BY SEQUENCE DESC LIMIT 1;' % self.table_name()
        self._conn.ExecSQL(query)
        row = self._conn.cursor.fetchone()
        if row:
            self._sequence = int(row[0])
        else:
            self._sequence = 0

        return self._sequence

    def register_session(self,files_v,status=1,check=True):

        if check:
            for f in files_v:
                if not os.path.isfile(f):
                    print "File not found:",f
                    raise Exception

        session = self.max_session_id() + 1

        for f in files_v:
            self.fill(f,session,int(status))

    def count_jobs(self,sequence=None):
        
        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        query = 'SELECT COUNT(DISTINCT(JOB_INDEX)) FROM %s WHERE SEQUENCE=%d AND JOB_INDEX >= 0;' % (self.table_name(),sequence)
        self._conn.ExecSQL(query)

        try:
            row = self._conn.cursor.fetchone()
            return int(row[0])
        except TypeError,IndexError:
            return 0

    def fill(self,filepath,session_id,status=1,sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        if sequence is not None:

            sequence = int(sequence)

            if sequence < self._sequence:

                sys.stderr.write('Cannot assign a sequence id that is older than current one (%d)\n' % self._sequence)
                raise Exception

            self._sequence = sequence

        query  = 'INSERT INTO %s (sequence, session_id, status, filepath, job_index, joblock) VALUES ' % self.table_name()
        query += '(%d, %d, %d, \'%s\', -1, 0);' % (int(self._sequence), int(session_id), int(status), str(filepath))
        
        self._conn.ExecSQL(query)

    def list(self, session_id=None, status=None, job_index=None, lock=None, sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        query = 'SELECT SESSION_ID, JOB_INDEX, JOBLOCK, STATUS, FILEPATH, TIME_STAMP FROM %s WHERE SEQUENCE = %d'
        query = query % (self.table_name(),sequence)

        condition_v = []
        if session_id is not None:
            condition_v.append('SESSION_ID = %d' % int(session_id))
        if status is not None:
            condition_v.append('STATUS = %d' % int(status))
        if job_index is not None:
            condition_v.append('JOB_INDEX = %d' % int(job_index))
        if lock is not None:
            if lock:
                condition_v.append('JOBLOCK > 0')
            else:
                condition_v.append('JOBLOCK < 1')

        for c in condition_v:

            query += ' AND %s' % c
            
        query += ' ORDER BY SESSION_ID, FILEPATH;'

        self._conn.ExecSQL(query)

        res = []
        while 1:
            row = self._conn.cursor.fetchone()
            if not row: break
            res.append((int(row[0]),int(row[1]),bool(row[2]),int(row[3]),str(row[4]),str(row[5])))

        return res

    def max_session_id(self,sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        query = 'SELECT MAX(SESSION_ID) FROM %s WHERE SEQUENCE = %d;' % (self.table_name(),sequence)
        self._conn.ExecSQL(query)

        try:
            row = self._conn.cursor.fetchone()
            return int(row[0])
        except TypeError,IndexError:
            return -1

    def unique_sessions(self, status=None, sequence=None):
        
        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        query = ''
        if status is not None:
            query = 'SELECT DISTINCT(SESSION_ID) FROM %s WHERE SEQUENCE = %d AND STATUS = %d'
            query = query % (self.table_name(),sequence,int(status))
        else:
            query = 'SELECT DISTINCT(SESSION_ID) FROM %s WHERE SEQUENCE = %d'
            query = query % (self.table_name(),sequence)

        query += ' ORDER BY SESSION_ID;'

        self._conn.ExecSQL(query)

        res = []

        while 1:
            row = self._conn.cursor.fetchone()
            if not row: break
            res.append(int(row[0]))

        return res

    def reset_job_index(self, status, sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        query = 'SELECT COUNT(FILEPATH) FROM %s WHERE JOBLOCK > 0 ;' % self.table_name()
        self._conn.ExecSQL(query)
        lock_exist=False
        res = self._conn.cursor.fetchone()
        if res and bool(res[0]):
            lock_exist=True
        
        if lock_exist:
            sys.stderr.write('ERROR: there are locked entries in this sequence!')
            raise Exception

        query = 'UPDATE %s SET JOB_INDEX = -1;' % self.table_name()
        self._conn.ExecSQL(query)
        
        sessions = self.unique_sessions(status,sequence)

        sessions.sort()

        for jid in xrange(len(sessions)):
            query = 'UPDATE %s SET JOB_INDEX = %d, JOBLOCK = 1 WHERE SEQUENCE = %d AND SESSION_ID = %d;'
            query = query % (self.table_name(), jid, sequence, sessions[jid])
            self._conn.ExecSQL(query)

    def is_locked(self,sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)
        
        query = 'SELECT COUNT(JOB_INDEX) FROM %s WHERE SEQUENCE = %d AND JOBLOCK > 0;' % (self.table_name(),sequence)
        self._conn.ExecSQL(query)
        
        return (int(self._conn.cursor.fetchone()[0]) > 0)

    def unlock(self,sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)
        
        query = 'UPDATE %s SET JOBLOCK = 0 WHERE SEQUENCE = %d;' % (self.table_name(),sequence)
        self._conn.ExecSQL(query)

    def job_session(self,job_index,sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        query = 'SELECT DISTINCT(SESSION_ID) FROM %s WHERE SEQUENCE=%d AND JOB_INDEX=%d ORDER BY SESSION_ID;' 
        query = query % (self.table_name(), sequence, job_index)

        self._conn.ExecSQL(query)
        
        res=[]
        while 1:
            row = self._conn.cursor.fetchone()
            if not row: break
            res.append(int(row[0]))

        assert len(res) == 1

        return res[0]

    def job_files(self,job_index,sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        query = 'SELECT FILEPATH FROM %s WHERE SEQUENCE=%d AND JOB_INDEX=%d ORDER BY FILEPATH;' 
        query = query % (self.table_name(), sequence, job_index)

        self._conn.ExecSQL(query)
        
        res=[]
        while 1:
            row = self._conn.cursor.fetchone()
            if not row: break
            res.append(str(row[0]))

        return res

    def update_status(self,status,job_index=None,session_id=None,sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        query = 'UPDATE %s SET STATUS = %d WHERE SEQUENCE = %d' % (self.table_name(),int(status),sequence)

        condition_v = []
        if session_id is not None:
            condition_v.append('SESSION_ID = %d' % int(session_id))
        if job_index is not None:
            condition_v.append('JOB_INDEX = %d' % int(job_index))

        for c in condition_v:

            query += ' AND %s' % c
            
        query += ';'
        
        self._conn.ExecSQL(query)
        

