# Class definition:^M
#   module for RunJobKurchatovhpc2 which is used with pilot launcher for HPC ^M
# Import relevant python/pilot modules^M
import MySQLdb

# Pilot modules
from pUtil import tolog 
from ErrorDiagnosis import ErrorDiagnosis # import here to avoid issues seen at BU with missing module
from PilotErrors import PilotErrors
from hpcconf import MYDB
import hpcconf #import MYDB and ssh.server




class launcherDB():

    # private data members
    __conn = None
#    host='127.0.0.1'
#    port='3306'
#    db='pilot1'
#    user='pilot'
#    passwd='pandapilot'
    host=MYDB['host']
    port=MYDB['port']
    db=MYDB['db']
    user=MYDB['user']
    passwd=MYDB['passwd']   

 
    def __connect(self):
        try:
            if ( host is not None ) and ( port is not None ) and ( db is not None ) and ( user is not None ) and ( passwd is not None ): 
	        self.__conn = MySQLdb.connect(host=host, port=port, db=db, user=user, passwd=passwd)
            else:
		pilotErrorDiag = "Coudn't connect to DB undefined connect_string"
                tolog("!!WARNING!! undefined DB connect_string" )
        except Exception, e:
	    pilotErrorDiag = "Coudn't connect to DB: %s" % str(e)
            tolog("!!WARNING!! %s" % (pilotErrorDiag))

    def __disconnect(self):
        if __conn is not None:
	    __conn.close()

    def update_record_status(self, pid, status):
        try:
            sql = "UPDATE pilots SET status=\'%s\' WHERE id=%d"%(status,pid)
	    if __conn is None:
		self.__connect()
            cur  =__conn.cursor()
            cur.execute(sql, {})
            __conn.commit()
            cur.close()
	    #conn.close()
	    self.__disconnect()
        except Exception, e:
            pilotErrorDiag = "Coudn't update record in DB: %s" % str(e)
            tolog("!!WARNING!! %s" % (pilotErrorDiag))

    def create_record(self, pid, status, cpu=0, mem=0, hdd=0):
        try:
            sql = "INSERT INTO pilots (id, status, cpu, mem, hdd, server) VALUES (%d, '%s', %d, %d, %d, '%s')"%(pid, status, cpu, mem, hdd, hpcconf.ssh.server)
            if __conn is None:
                self.__connect()
            cur = __conn.cursor()
            cur.execute(sql, {})
            __conn.commit()
            cur.close()
            self.__disconnect()
            #conn.close()
        except Exception, e:
            pilotErrorDiag = "Coudn't add record in DB: %s" % str(e)
            tolog("!!WARNING!! %s" % (pilotErrorDiag))
