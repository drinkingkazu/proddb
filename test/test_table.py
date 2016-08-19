from  proddb.table import table
import sys

t=table('test_removeme')

# test creation
t.create()
if not t.exist():
    print 'Failed creation'
    sys.exit(1)
t.drop()
t.create()

t.fill('aho_000.root',0)
t.fill('aho_001.root',1)

assert len(t.list()) == 2
assert len(t.list(session_id=0)) == 1
assert len(t.list(status=0)) == 2
assert len(t.list(job_index=0)) == 0
assert len(t.list(job_index=-1)) == 2
assert len(t.list(lock=True)) == 0
assert len(t.list(lock=False)) == 2
assert len(t.unique_sessions(sequence=0)) == 2
assert len(t.unique_sessions(sequence=1)) == 0
assert len(t.unique_sessions()) == 2

t.reset_job_index(status=0)
assert len(t.list(job_index=0)) == 1
assert len(t.list(job_index=1)) == 1
assert len(t.list(job_index=-1)) == 0
assert t.count_jobs() == 2
try:
    t.reset_job_index()
    sys.exit(1)
except Exception:
    pass

t.unlock()
t.reset_job_index(status=1)
assert len(t.list(job_index=0)) == 0
assert len(t.list(job_index=-1)) == 2

t.unlock()
t.reset_job_index(status=0)
assert t.job_session(job_index=0) == 0
assert t.job_session(job_index=1) == 1
assert len(t.job_files(job_index=0)) == 1

t.unlock()
t.update_status(1)
t.reset_job_index(status=0)
assert len(t.list(job_index=-1)) == 2

assert t.max_session_id() == 1
assert t.max_session_id(1) == 0

t.drop()
