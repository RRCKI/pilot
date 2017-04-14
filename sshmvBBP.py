import os, re
import commands
from time import time

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand, getSiteInformation, getExperiment
from FileStateClient import updateFileState

# placing the import lfc here breaks compilation on non-lcg sites
# import lfc

import epic
import shlex
import pipes
import Configuration
from config import config_sm
import hpcconf

ARCH_DEFAULT = config_sm.ARCH_DEFAULT
CMD_CHECKSUM = config_sm.COMMAND_MD5

class sshmvSiteMover(SiteMover.SiteMover):
    """ SiteMover that uses lcg-cp for both get and put """
    # no registration is done
    copyCommand = "sshmvbbp"
    checksum_command = "adler32"
    has_mkdir = False
    has_df = False
    has_getsize = False
    has_md5sum = True
    has_chmod = False
    timeout = 3600

    def __init__(self, setup_path, *args, **kwrds):
        self._setup = setup_path

    def get_timeout(self):
        return self.timeout

    def isNewLCGVersion(cmd):
        return True
    isNewLCGVersion = staticmethod(isNewLCGVersion)

    def adler32(filename):
        #epic.ssh('python ~/bin/adler32cmd.py %s' %(filename))
        #return epic.output.strip()
        return "ZZZZZZZZ"
    adler32 = staticmethod(adler32)

    def getLocalFileInfo(fname, csumtype="default", date=None):
        fname=os.path.abspath(fname)
        #epic.ssh('python ~/bin/fileinfos.py %s' %(fname))
        pilotErrorDiag=''
        #arr=epic.output.split("\n")
        #fsize=arr[1].split(':',1)
        #fsize=fsize[1]
	fsize = "1"

        error = PilotErrors()
        pilotErrorDiag = ""

        # get the checksum
        if csumtype == "adler32":
            tolog("Executing adler32() for file: %s" % (fname))
            fchecksum = sshmvSiteMover.adler32(fname)
            if fchecksum == '00000001': # "%08x" % 1L
                pilotErrorDiag = "Adler32 failed (returned 1)"
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_FAILEDADLOCAL, pilotErrorDiag, fsize, 0
            else:
                tolog("Got adler32 checksum: %s" % (fchecksum))
        else:
            _cmd = '%s %s' % (CMD_CHECKSUM, fname)
            tolog("Executing command: %s" % (_cmd))
            epic.ssh(_cmd)
            o=epic.output.strip()
            s=epic.exit_code
            if s != 0:
                o = o.replace('\n', ' ')
                check_syserr(s, o)
                pilotErrorDiag = "Error running checksum command (%s): %s" % (CMD_CHECKSUM, o)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_FAILEDMD5LOCAL, pilotErrorDiag, fsize, 0
            fchecksum = o.split()[0]
            tolog("Got checksum: %s" % (fchecksum))
        return 0, pilotErrorDiag, fsize, fchecksum

    getLocalFileInfo = staticmethod(getLocalFileInfo)

    def getTier3Path(dsname, DN):
        dsname = dsname.replace(':','/').replace('//','/')
        return os.path.join(hpcconf.SEpath,dsname)
    getTier3Path = staticmethod(getTier3Path)


    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ copy input file from SE to local dir """
        dsname = pdict.get('dsname', '').replace(':','/').replace('//','/')
        print("mylog_fields123: %s, %s, %s"%(gpfn, lfn, path))

        getfile = os.path.join(hpcconf.SEpath,dsname, lfn)

        error = PilotErrors()
        pilotErrorDiag = ""

        if path == '': path = './'
        fullname = os.path.join(path, lfn)

        epic.ssh('mkdir -p %s;ln -s %s %s' %(path,getfile,fullname))
        o=epic.output
        s=epic.exit_code
        tolog("Command finished: %s" % ( o))

        if s != 0:
            pilotErrorDiag = "Error linking the file: %d, %s" % (s, o)
            tolog('!!WARNING!!2100!! %s' % (pilotErrorDiag))
            return error.ERR_STAGEINFAILED, pilotErrorDiag

        return 0, pilotErrorDiag

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """ copy output file from disk to local SE """
        dsname = pdict.get('dsname', '').replace(':','/').replace('//','/')
        if '.log.' in source:
            fp=os.path.abspath(source)
            epic.push_file(fp,fp)
        print("mylog_fields123: %s, %s"%(source, destination))
        filename = os.path.basename(source)

        putfile_path = os.path.join(hpcconf.SEpath,dsname)
        putfile=os.path.join(putfile_path,filename)

        ec,ped,fsize,fchecksum=self.getLocalFileInfo(source,'adler32')

        cmd='mkdir -p '+putfile_path+"\nmv %s %s"%(source,putfile)

        error = PilotErrors()
        pilotErrorDiag = ""

        epic.ssh(cmd)
        o=epic.output
        ec=epic.exit_code

        if ec != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (cmd))
            tolog('!!WARNING!!2990!! put_data failed: Status=%d Output=%s' % (ec, str(o)))
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        return 0, pilotErrorDiag, putfile, fsize, fchecksum, ARCH_DEFAULT
