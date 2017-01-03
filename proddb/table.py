from dbconn import dbconn
from dbenv import *
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
        query += 'filepath   VARCHAR(200),'
        query += 'extra_data VARCHAR(200)'
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

    def last_timestamp(self,sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        query = 'SELECT MAX(TIME_STAMP) FROM %s WHERE SEQUENCE = %d;' % (self.table_name(),sequence)

        self._conn.ExecSQL(query)

        row = self._conn.cursor.fetchone()
        if not row: return ''
        else: return row[0]

    def describe(self):

        num_sessions = self.count_sessions()
        num_files = self.count_files()
        job_info = self.job_info()
        last_timestamp = self.last_timestamp()

        msg = '%d sessions, %d files ... last update @ %s' % (num_sessions,num_files,last_timestamp)
        for jobid,status_v in job_info.iteritems():
            temp_msg = '(%d init, %d submitted, %d running, %d done, %d failed)'
            temp_msg = temp_msg % (len(status_v[kSTATUS_INIT]),
                                   len(status_v[kSTATUS_SUBMIT]),
                                   len(status_v[kSTATUS_RUNNING]),
                                   len(status_v[kSTATUS_DONE]),
                                   len(status_v[kSTATUS_FAILED]))
            if jobid is None or jobid is 'None': 
                msg += '\nNot for job submission: ' + temp_msg
            else:
                numjobs  = len(status_v[kSTATUS_INIT]) + len(status_v[kSTATUS_SUBMIT]) + len(status_v[kSTATUS_RUNNING])
                numjobs += len(status_v[kSTATUS_DONE]) + len(status_v[kSTATUS_FAILED])
                msg += '\nJob ID %s ... %d jobs : ' % (jobid,numjobs) + temp_msg
        return msg

    def register_session(self,files_v,status=None,session=None,check=True):
        
        if status is None: status = kSTATUS_INIT

        if check:
            for f in files_v:
                if not os.path.isfile(f):
                    print "File not found:",f
                    raise Exception

        if session is None:
            session = self.max_session_id() + 1

        for f in files_v:
            self.fill(f,session,int(status))

    def register_jobid(self,jobid,job_index=None):

        sequence = self.sequence()
        
        query = 'UPDATE %s SET EXTRA_DATA=\'%s\' WHERE SEQUENCE=%d' % (self.table_name(),str(jobid),sequence)
        if job_index is not None:
            query += ' AND JOB_INDEX = %d' % int(job_index)

        query += ';'
        
        self._conn.ExecSQL(query)

    def job_info(self,sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        query = 'SELECT DISTINCT(EXTRA_DATA) FROM %s WHERE SEQUENCE=%d;' % (self.table_name(),sequence)
        self._conn.ExecSQL(query)

        jobid_v=[]
        while 1:
            row = self._conn.cursor.fetchone()
            if not row: break
            if row[0] is None or row[0] == 'None': break
            jobid_v.append(row[0])

        query_format = 'SELECT JOB_INDEX, STATUS FROM %s WHERE SEQUENCE=%d AND EXTRA_DATA=\'%s\' GROUP BY JOB_INDEX;'
        res = {}
        for jobid in jobid_v:

            query = query_format % (self.table_name(),sequence,jobid)
            self._conn.ExecSQL(query)

            oneres={kSTATUS_INIT    : [],
                    kSTATUS_SUBMIT  : [],
                    kSTATUS_RUNNING : [],
                    kSTATUS_DONE    : [],
                    kSTATUS_FAILED  : []}
            while 1:
                row = self._conn.cursor.fetchone()
                if not row: break
                job_index = int(row[0])
                status = int(row[1])
                if not status in oneres: oneres[status]=[]
                oneres[status].append(job_index)
            res[jobid]=oneres
        return res

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

    def count_sessions(self,sequence=None):
        
        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)
        
        query = 'SELECT COUNT(DISTINCT(SESSION_ID)) FROM %s WHERE SEQUENCE=%d;' % (self.table_name(),sequence)
        self._conn.ExecSQL(query)

        try:
            row = self._conn.cursor.fetchone()
            return int(row[0])
        except TypeError,IndexError:
            return 0

    def count_files(self,sequence=None):
        
        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)
        
        query = 'SELECT COUNT(DISTINCT(FILEPATH)) FROM %s WHERE SEQUENCE=%d;' % (self.table_name(),sequence)
        self._conn.ExecSQL(query)

        try:
            row = self._conn.cursor.fetchone()
            return int(row[0])
        except TypeError,IndexError:
            return 0

    def count_status(self,sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        query = 'SELECT DISTINCT(STATUS), COUNT(STATUS) FROM %s WHERE SEQUENCE = %s;' % (self.table_name(),sequence)
        self._conn.ExecSQL(query)
        res={}
        while 1:
            row = self._conn.cursor.fetchone()
            if not row: break
            res[int(row[0])] = int(row[1])
        return res

    def diff_session(self,project):

        t=table(project)
        if not t.exis():
            print 'ERROR: project does not exist',project
            return ((),())
        t.close()

        query  = ' SELECT A.SESSION_ID, B.SESSION_ID from %s AS A LEFT JOIN %s AS B' % (self.table_name(),t.table_name())
        query += ' ON B.SESSION_ID=A.SESSION_ID WHERE B.SESSION_ID IS NULL GROUP BY A.SESSION_ID;'
        self._conn.ExecSQL(query)
        a2b = []
        while 1:
            row = self._conn.cursor.fetchone()
            if not row: break
            a2b.append(int(row[0]))

        query  = ' SELECT A.SESSION_ID, B.SESSION_ID from %s AS A LEFT JOIN %s AS B' % (t.table_name(),self.table_name())
        query += ' ON B.SESSION_ID=A.SESSION_ID WHERE B.SESSION_ID IS NULL GROUP BY A.SESSION_ID;'
        self._conn.ExecSQL(query)
        b2a = []
        while 1:
            row = self._conn.cursor.fetchone()
            if not row: break
            b2a.append(int(row[0]))
        return (tuple(a2b),tuple(b2a))

    def fill(self,filepath,session_id,status=None,sequence=None):

        if status is None: status = kSTATUS_INIT

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

        query = 'SELECT SESSION_ID, JOB_INDEX, JOBLOCK, STATUS, FILEPATH, TIME_STAMP, EXTRA_DATA FROM %s WHERE SEQUENCE = %d'
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
            res.append((int(row[0]),int(row[1]),bool(row[2]),int(row[3]),str(row[4]),str(row[5]),str(row[6])))

        return res

    def list_jobs(self, sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        query = 'SELECT JOB_INDEX, STATUS, EXTRA_DATA, TIME_STAMP FROM %s WHERE SEQUENCE = %d AND JOB_INDEX >=0 '
        query = query % (self.table_name(),sequence)
        query += ' GROUP BY JOB_INDEX ORDER BY JOB_INDEX;'

        self._conn.ExecSQL(query)

        res = []
        while 1:
            row = self._conn.cursor.fetchone()
            if not row: break
            res.append((int(row[0]),int(row[1]),str(row[2]),str(row[3])))

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

    def update_status(self,status,target_status=None,job_index=None,session_id=None,extra_data=None,sequence=None):

        if sequence is None: sequence = self.sequence()
        sequence = int(sequence)

        query = 'UPDATE %s SET STATUS = %d WHERE SEQUENCE = %d' % (self.table_name(),int(status),sequence)

        condition_v = []
        if session_id is not None:
            condition_v.append('SESSION_ID = %d' % int(session_id))
        if job_index is not None:
            condition_v.append('JOB_INDEX = %d' % int(job_index))
        if target_status is not None:
            condition_v.append('STATUS = %d' % int(target_status))
        if extra_data is not None:
            condition_v.append('EXTRA_DATA=\'%s\'' % str(extra_data))

        for c in condition_v:

            query += ' AND %s' % c
            
        query += ';'
        
        self._conn.ExecSQL(query)

    def update_job_status(self,status,job_index=None):

        sequence = self.sequence()

        query = 'UPDATE %s SET STATUS = %d WHERE SEQUENCE = %d' % (self.table_name(),int(status),sequence)

        condition_v = []
        if job_index is not None:
            condition_v.append('JOB_INDEX = %d' % int(job_index))
        else:
            condition_v.append('JOB_INDEX >= 0')

        for c in condition_v:

            query += ' AND %s' % c
            
        query += ';'
        
        self._conn.ExecSQL(query)

    def regroup_sessions(self,modular,status=None,sequence=None):

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

        query = 'SELECT ID FROM %s WHERE sequence = %d ' % (self.table_name(),sequence)
        if status is not None:
            query += ' AND STATUS = %d;' % int(status)
            
        self._conn.ExecSQL(query)
        keys=[]

        while 1:
            row = self._conn.cursor.fetchone()
            if not row: break
            keys.append( (int(len(keys)/int(modular)), int(row[0]) ) )

        for key in keys:
            query  = 'UPDATE %s ' % self.table_name()
            query += ' SET SESSION_ID = %d WHERE ID = %d;' % key
            self._conn.ExecSQL(query)

