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
import json

prefixpath='/data/pilots'
cloudprefix='/httpcloud'
curl='curl'
curl_args='--silent --show-error --connect-timeout 100 --max-time 120 --insecure --compressed'
server='https://vcloud23.grid.kiae.ru:8060/api/file'
ARCH_DEFAULT = config_sm.ARCH_DEFAULT
CMD_CHECKSUM = config_sm.COMMAND_MD5

class sshcurlSiteMover(SiteMover.SiteMover):
    """ SiteMover that uses lcg-cp for both get and put """
    # no registration is done
    copyCommand = "sshcurl"
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

    def isCloud(file):
        global cloudprefix
        return os.path.commonprefix([file,cloudprefix])==cloudprefix
    isCloud = staticmethod(isCloud)

    def adler32(filename):
        if sshcurlSiteMover.isCloud(filename):
            s,e,fs,sum=sshcurlSiteMover.getLocalFileInfo(filename,'adler32')
            if s!=0: return '00000001'
            return sum
        epic.ssh('python ~/bin/adler32cmd.py %s' %(filename))
        return epic.output.strip()
    adler32 = staticmethod(adler32)

    def getLocalFileInfo(fname, csumtype="default", date=None):
        fname=os.path.abspath(fname)
        if sshcurlSiteMover.isCloud(fname):
            global curl,curl_args,server
            fname=fname[len(cloudprefix):]
            cmd=curl+' '+curl_args+' '+server+fname+'/info'
            tolog('Executing:'+cmd)
            s,o = commands.getstatusoutput(cmd)
            if s!=0:
                pilotErrorDiag = "CURL failed: "+o
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return s, pilotErrorDiag, 0, 0

            obj=json.loads(o)
            if csumtype=='adler32':
                return 0, '', obj['fsize'], obj['adler32']
            return 0, '',obj['fsize'], obj['md5sum']
        epic.ssh('python ~/bin/fileinfos.py %s' %(fname))
        pilotErrorDiag=''
        arr=epic.output.split("\n")
        fsize=arr[1].split(':',1)
        fsize=fsize[1]

        error = PilotErrors()
        pilotErrorDiag = ""

        # get the checksum
        if csumtype == "adler32":
            tolog("Executing adler32() for file: %s" % (fname))
            fchecksum = sshcurlSiteMover.adler32(fname)
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
        global prefixpath,cloudprefix
        # return '/s/ls2/users/poyda/data/se'
        dsname = dsname[dsname.find(':')+1:]
        return os.path.join(cloudprefix,dsname)
    getTier3Path = staticmethod(getTier3Path)


    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ copy input file from SE to local dir """
        global cloudprefix,curl,curl_args,server
        dsname = pdict.get('dsname', '')
        dsname=dsname[dsname.find(':')+1:]
        print("mylog_fields123: %s, %s, %s"%(gpfn, lfn, path))

        getfile = os.path.join(dsname, lfn)

        error = PilotErrors()
        pilotErrorDiag = ""

        if path == '': path = './'
        fullname = os.path.join(path, lfn)

        epic.ssh('mkdir -p %s;%s %s -o %s %s/%s/fetch' %(path,curl,curl_args,fullname,server,getfile))
        o=epic.output
        s=epic.exit_code
        tolog('Returned status: Status=%d Output=%s' % (s, str(o)))

        if s != 0:
            pilotErrorDiag = "Error copying the file: %d, %s" % (s, o)
            tolog('!!WARNING!!2100!! %s' % (pilotErrorDiag))
            return error.ERR_STAGEINFAILED, pilotErrorDiag

        return 0, pilotErrorDiag

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """ copy output file from disk to local SE """
        global prefixpath,curl_args,curl,server,cloudprefix
        dsname = pdict.get('dsname', '')
        dsname=dsname[dsname.find(':')+1:]
        print("mylog_fields123: %s, %s"%(source, destination))
        filename = os.path.basename(source)

        putfile=os.path.join(dsname,filename)
        # cmd='mkdir -p '+putfile_path+"\nmv %s %s"%(source,putfile)

        error = PilotErrors()
        pilotErrorDiag = ""

        cmd='%s -X POST %s --header "Content-Type:application/octet-stream" --data-binary @%s %s/%s/save' %(curl,curl_args,source,server,putfile)

        if '.log.' in source:
            ec,ped,fsize,fchecksum=SiteMover.SiteMover.getLocalFileInfo(source,'adler32')
            tolog('Executing:'+cmd)
            ec,o=commands.getstatusoutput(cmd)
        else:
            ec,ped,fsize,fchecksum=self.getLocalFileInfo(source,'adler32')
            epic.ssh(cmd)
            o=epic.output
            ec=epic.exit_code


        tolog("Curl command sent: %s" % (cmd))
        tolog('Returned status: Status=%d Output=%s' % (ec, str(o)))
        if ec != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (cmd))
            tolog('!!WARNING!!2990!! put_data failed: Status=%d Output=%s' % (ec, str(o)))
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        return 0, pilotErrorDiag, putfile, fsize, fchecksum, ARCH_DEFAULT
