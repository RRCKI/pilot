from __future__ import print_function
import time
import os
import fcntl
import errno
import commands
import pipes
from pUtil import tolog

class SimpleFlock:
   """Provides the simplest possible interface to flock-based file locking. Intended for use with the `with` syntax. It will create/truncate/delete the lock file as necessary."""

   def __init__(self, path, timeout = None,number_locks=7):
       self._pathbase = os.path.expanduser(path)
       self._timeout = timeout
       self._fd = None
       self._number_locks=number_locks
       self._obtained_time=False

   def __enter__(self):
      start_lock_search = time.time()
      num=0
      while True:
         try:
             num+=1
             if num>self._number_locks:
                 num=0
             self._path=self._pathbase+(".%d"%num)
             self._fd = os.open(self._path, os.O_CREAT)
             fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
             # Lock acquired!
             time.sleep(0.5)
             self._obtained_time=time.time()
             return
         except (OSError, IOError) as ex:
             if ex.errno != errno.EAGAIN: # Resource temporarily unavailable
                raise
             elif self._timeout is not None and time.time() > (start_lock_search + self._timeout):
                tolog("Lock wait timeout exceeded")
                o=commands.getoutput("lsof "+pipes.quote(self._pathbase)+".*")
                tolog("list all locks:\n"+o)
                # Exceeded the user-specified timeout.
                raise
             try:
                 os.unlink(self._path)
             except:
                pass

         
         # TODO It would be nice to avoid an arbitrary sleep here, but spinning
         # without a delay is also undesirable.
         time.sleep(0.1)

   def __exit__(self, *args):
        if self._obtained_time is not False:
            tolog("lock was set for %s seconds"%(time.time()-self._obtained_time))
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None

        # Try to remove the lock file, but don't try too hard because it is
        # unnecessary. This is mostly to help the user see whether a lock
        # exists by examining the filesystem.
        try:
            os.unlink(self._path)
        except:
            pass

if __name__ == "__main__":
   print("Acquiring lock...")
   with SimpleFlock("locktest", 2):
      print("Lock acquired.")
      time.sleep(3)
   print("Lock released.")



