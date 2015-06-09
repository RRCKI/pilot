"""This code was modified and adopted to work for Kurchatov HPC"""

import os, re, sys
import commands, posixpath
import saga
from time import time

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand, getSiteInformation, getExperiment
from FileStateClient import updateFileState
from tempfile import mktemp

# placing the import lfc here breaks compilation on non-lfc sites
# import lfc
import epic


source_cvmfs = "source /adm/scripts/grid/emi-wn-cvmfs-environment.sh"

class sshlcgcpSiteMover(SiteMover.SiteMover):
    """ SiteMover for lcg-cp """

    copyCommand = "sshlcgcp"
    checksum_command = "adler32"
    has_mkdir = False
    has_df = False
    has_getsize = False
    has_md5sum = True
    has_chmod = False
    timeout = 3600
    remote_prefix = "/s/ls2/groups/g0037/panda/"

    # private data members
    __saga_context = None
    __saga_session = None
    __saga_service = None

    #__error = PilotErrors()                    # PilotErrors object

    # public members
    ssh_user="poyda"
    ssh_keypath="/home/apf/.ssh/sk_poyda_rsa"
    ssh_pass=""
    ssh_server="ui2.computing.kiae.ru"
    partition_comp = 'bamboo-1w'

    PartitionWalltimePerNode=3200

    def initSaga(self):
        """ inits Saga context, session and service """
        if not self.__saga_service:
            try:
                self.__saga_context=saga.Context("ssh")
                self.__saga_context.user_id   = self.ssh_user
                self.__saga_context.user_cert = self.ssh_keypath # private key derived from cert
                self.__saga_context.user_pass = self.ssh_pass

                self.__saga_session = saga.Session()
                self.__saga_session.add_context(self.__saga_context)

                self.__saga_service = saga.job.Service("slurm+ssh://%s" % self.ssh_server,
                                  session=self.__saga_session)

            except saga.SagaException, ex:
                # Catch all saga exceptions
                tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
                # Trace back the exception. That can be helpful for debugging.
                tolog(" \n*** Backtrace:\n %s" % ex.traceback)
                self.__saga_service=None

        return self.__saga_service

    def __init__(self, setup_path, *args, **kwrds):
        self._setup = setup_path
        self.initSaga()

    def get_timeout(self):
        return self.timeout

    def check_space(self, ub):
        """ For when space availability is not verifiable """
        return 999999

    def core_get_data(self, envsetup, token, source_surl, dest_path, experiment):
        """ stage-in core function, can be overridden (see stormSiteMover) """
        global source_cvmfs

        error = PilotErrors()

        timeout_option = "--srm-timeout=%d --connect-timeout=300 --sendreceive-timeout=%d" % (self.timeout, self.timeout)

        # used lcg-cp options:
        # --vo: specifies the Virtual Organization the user belongs to
        #   -t: time-out

        _cmd_str =source_cvmfs+ '\n%s lcg-cp --vo atlas -b %s -T srmv2 %s file://%s' % (envsetup, timeout_option, source_surl, dest_path)

        # get the experiment object
        thisExperiment = getExperiment(experiment)

        # add the full stage-out command to the job setup script
        to_script = _cmd_str.replace("file://%s" % posixpath.dirname(dest_path), "file://`pwd`")
        to_script = to_script.lstrip(' ') # remove any initial spaces
        if to_script.startswith('/'):
            to_script = 'source ' + to_script
        thisExperiment.updateJobSetupScript(posixpath.dirname(dest_path), to_script=to_script)

        tolog("Executing command: %s" % (_cmd_str))
        s = -1
        o = '(not defined)'
        t0 = os.times()

        epic.slurm(_cmd_str)
        o=epic.output
        s=epic.exit_code
        #
        # tolog("******* SAGA call to stage-in *********")
        # try:
        #     js = self.initSaga()
        #
        #     jd = saga.job.Description()
        #     # jd.project = self.project_id # should be taken from resourse description (pandaqueue)
        #     jd.wall_time_limit = 600
        #     jd.executable      = _cmd_str
        #     jd.total_cpu_count = 1
        #     jd.queue = self.partition_comp
        #     jd.working_directory = self.remote_prefix
        #
        #     fork_job = js.create_job(jd)
        #     fork_job.add_callback(saga.STATE, self.jobStateChangeNotification)
        #
        #     #tolog("\n(PBS) Command: %s\n"  % to_script)
        #     fork_job.run()
        #     tolog("Local Job ID: %s" % fork_job.id)
        #
        #
        #     fork_job.wait()
        #     s=fork_job.state
        # except saga.SagaException, ex:
        #     # Catch all saga exceptions
        #     tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        #     # Trace back the exception. That can be helpful for debugging.
        #     tolog(" \n*** Backtrace:\n %s" % ex.traceback)
        #
        # (s,o)={
        #     saga.job.NEW:(-4,"New"),
        #     saga.job.PENDING:(-3,"Pending"),
        #     saga.job.RUNNING:(-2,"Running"),
        #     saga.job.DONE:(0,"OK"),
        #     saga.job.FAILED:(1,"Failed"),
        #     saga.job.CANCELED:(2,"Canceled"),
        #     saga.job.SUSPENDED:(3,"Suspended")
        # }.get(s,(-1,"Unknown"))
        # tolog("******* SAGA call ended with status %s *********"%(o))

        t1 = os.times()
        t = t1[4] - t0[4]
        tolog("Command finished after %f s" % (t))
        if s == 0:
            tolog("get_data succeeded")
        else:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            o = o.replace('\n', ' ')
            check_syserr(s, o)
            tolog("!!WARNING!!2990!! get_data failed. Status=%s Output=%s" % (str(s), str(o)))
            if o.find("globus_xio:") >= 0:
                pilotErrorDiag = "Globus system error: %s" % (o)
                tolog("Globus system error encountered")
                return error.ERR_GETGLOBUSSYSERR, pilotErrorDiag
            elif o.find("No space left on device") >= 0:
                pilotErrorDiag = "No available space left on local disk: %s" % (o)
                tolog("No available space left on local disk")
                return error.ERR_NOLOCALSPACE, pilotErrorDiag
            elif o.find("No such file or directory") >= 0:
                if source_surl.find("DBRelease") >= 0:
                    pilotErrorDiag = "Missing DBRelease file: %s" % (source_surl)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    return error.ERR_MISSDBREL, pilotErrorDiag
                else:
                    pilotErrorDiag = "No such file or directory: %s" % (source_surl)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    return error.ERR_NOSUCHFILE, pilotErrorDiag
            else:
                if t >= self.timeout:
                    pilotErrorDiag = "Copy command self timed out after %d s" % (t)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    return error.ERR_GETTIMEOUT, pilotErrorDiag
                else:
                    if len(o) == 0:
                        pilotErrorDiag = "Copy command returned error code %d but no output" % (s)
                    else:
                        pilotErrorDiag = o
                    return error.ERR_STAGEINFAILED, pilotErrorDiag
        return None

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ copy input file from SE to local dir """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        token = pdict.get('token', None)
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        experiment = pdict.get('experiment', '')
        proxycheck = pdict.get('proxycheck', False)

        # try to get the direct reading control variable (False for direct reading mode; file should not be copied)
        useCT = pdict.get('usect', True)
        prodDBlockToken = pdict.get('access', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'lcg', lfn, guid)

        # get a proper envsetup
        envsetup = self.getEnvsetup(get=True)

        ec, pilotErrorDiag = verifySetupCommand(error, envsetup)
        if ec != 0:
            self.__sendReport('RFCP_FAIL', report)
            return ec, pilotErrorDiag

        # get the experiment object
        thisExperiment = getExperiment(experiment)

        if proxycheck:
            # do we have a valid proxy?
            s, pilotErrorDiag = thisExperiment.verifyProxy(envsetup=envsetup)
            if s != 0:
                self.__sendReport('PROXYFAIL', report)
                return s, pilotErrorDiag
        else:
            tolog("Proxy verification turned off")

        getfile = gpfn

        if path == '': path = './'
        fullname = os.path.join(path, lfn)

        # should the root file be copied or read directly by athena?
        directIn, useFileStager = self.getTransferModes()
        if directIn:
            if useCT:
                directIn = False
                tolog("Direct access mode is switched off (file will be transferred with the copy tool)")
                updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", type="input")
            else:
                # determine if the file is a root file according to its name
                rootFile = self.isRootFileName(lfn)

                if prodDBlockToken == 'local' or not rootFile:
                    directIn = False
                    tolog("Direct access mode has been switched off for this file (will be transferred with the copy tool)")
                    updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", type="input")
                elif rootFile:
                    tolog("Found root file according to file name: %s (will not be transferred in direct reading mode)" % (lfn))
                    report['relativeStart'] = None
                    report['transferStart'] = None
                    self.__sendReport('FOUND_ROOT', report)
                    if useFileStager:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="file_stager", type="input")
                    else:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="remote_io", type="input")
                    return error.ERR_DIRECTIOFILE, pilotErrorDiag
                else:
                    tolog("Normal file transfer")

        # get remote filesize and checksum
        if fsize == 0 or fchecksum == 0:
            try:
                import lfc
            except Exception, e:
                pilotErrorDiag = "get_data() could not import lfc module: %s" % str(e)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                self.__sendReport('LFC_IMPORT', report)
                return error.ERR_GETLFCIMPORT, pilotErrorDiag

            os.environ['LFC_HOST'] = readpar('lfchost')
            try:
                ret, res = lfc.lfc_getreplicas([str(guid)],"")
            except Exception, e:
                pilotErrorDiag = "Failed to get LFC replicas: %s" % str(e)
                tolog("!!WARNING!!2990!! Exception caught: %s" % (pilotErrorDiag))
                tolog("Mover get_data finished (failed)")
                self.__sendReport('NO_LFC_REPS', report)
                return error.ERR_FAILEDLFCGETREPS, pilotErrorDiag
            if ret != 0:
                pilotErrorDiag = "Failed to get replicas: %d, %s" % (ret, res)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.__sendReport('NO_REPS', report)
                return error.ERR_FAILEDLFCGETREPS, pilotErrorDiag
            else:
                # extract the filesize and checksum
                try:
                    fsize = res[0].filesize
                    fchecksum = res[0].csumvalue
                except Exception, e:
                    pilotErrorDiag = "lfc_getreplicas did not return filesize/checksum: %s" % str(e)
                    tolog("!!WARNING!!2990!! Exception caught: %s" % (pilotErrorDiag))
                    self.__sendReport('NO_LFC_FS_CS', report)
                    return error.ERR_FAILEDLFCGETREPS, pilotErrorDiag
                else:
                    tolog("filesize: %s" % str(fsize))
                    tolog("checksum: %s" % str(fchecksum))

        # invoke the transfer commands
        report['relativeStart'] = time()
        report['transferStart'] = time()
        result = self.core_get_data(envsetup, token, getfile, fullname, experiment)
        report['validateStart'] = time()
        if result:
            self.__sendReport('CORE_FAIL', report)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(fullname)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return result

        # get the checksum type (md5sum or adler32)
        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        if (fsize != 0 or fchecksum != 0) and self.doFileVerifications():
            loc_filename = lfn
            dest_file = os.path.join(path, loc_filename)

            # get the checksum type (md5sum or adler32)
            if fchecksum != 0 and fchecksum != "":
                csumtype = self.getChecksumType(fchecksum)
            else:
                csumtype = "default"

            # get remote file size and checksum 
            ec, pilotErrorDiag, dstfsize, dstfchecksum = self.getLocalFileInfo(dest_file, csumtype=csumtype)
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)

                # remove the local file before any get retry is attempted
                _status = self.removeLocal(fullname)
                if not _status:
                    tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

                return ec, pilotErrorDiag

            # compare remote and local file size
            if long(fsize) != 0 and long(dstfsize) != long(fsize):
                pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                                 (os.path.basename(gpfn), str(dstfsize), str(fsize))
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.__sendReport('FS_MISMATCH', report)

                # remove the local file before any get retry is attempted
                _status = self.removeLocal(fullname)
                if not _status:
                    tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

                return error.ERR_GETWRONGSIZE, pilotErrorDiag

            # compare remote and local file checksum
            if fchecksum and dstfchecksum != fchecksum and not self.isDummyChecksum(fchecksum):
                pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                 (csumtype, os.path.basename(gpfn), dstfchecksum, fchecksum)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))

                # report corrupted file to consistency server
                self.reportFileCorruption(gpfn)

                # remove the local file before any get retry is attempted
                _status = self.removeLocal(fullname)
                if not _status:
                    tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

                if csumtype == "adler32":
                    self.__sendReport('AD_MISMATCH', report)
                    return error.ERR_GETADMISMATCH, pilotErrorDiag
                else:
                    self.__sendReport('MD5_MISMATCH', report)
                    return error.ERR_GETMD5MISMATCH, pilotErrorDiag

        updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", type="input")
        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag

    def put_data(self, pfn, destination, fsize=0, fchecksum=0, dsname='', extradirs='', **pdict):
        """ copy output file from disk to local SE """
        global source_cvmfs

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        token = pdict.get('token', '')
        scope = pdict.get('scope', '')
        logFile = pdict.get('logFile', '')
        sitename = pdict.get('sitename', '')
        proxycheck = pdict.get('proxycheck', False)
        experiment = pdict.get('experiment', '')
        analysisJob = pdict.get('analJob', False)
        prodSourceLabel = pdict.get('prodSourceLabel', '')

        # get the site information object
        si = getSiteInformation(experiment)

        if prodSourceLabel == 'ddm' and analysisJob:
            tolog("Treating PanDA Mover job as a production job during stage-out")
            analysisJob = False

        filename = pfn.split('/')[-1]

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'lcg', lfn, guid)

        # is the dataset defined?
        if dsname == '':
            pilotErrorDiag = "Dataset name not specified to put_data"
            tolog('!!WARNING!!2990!! %s' % (pilotErrorDiag))
            self.__sendReport('DSN_UNDEF', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        # preparing variables
        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(pfn, csumtype="adler32")
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
                return self.put_data_retfail(ec, pilotErrorDiag) 

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        # get a proper envsetup
        envsetup = self.getEnvsetup()

        ec, pilotErrorDiag = verifySetupCommand(error, envsetup)
        if ec != 0:
            self.__sendReport('RFCP_FAIL', report)
            return self.put_data_retfail(ec, pilotErrorDiag) 

        # get the experiment object
        thisExperiment = getExperiment(experiment)

        # do we need to check the user proxy?
        if proxycheck:
            s, pilotErrorDiag = thisExperiment.verifyProxy(envsetup=envsetup, limit=2)
            if s != 0:
                self.__sendReport('PROXY_FAIL', report)
                return self.put_data_retfail(error.ERR_NOPROXY, pilotErrorDiag)
        else:
            tolog("Proxy verification turned off")

        # get all the proper paths
        ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl = si.getProperPaths(error, analysisJob, token, prodSourceLabel, dsname, filename, scope=scope)
        if ec != 0:
            self.__sendReport(tracer_error, report)
            return self.put_data_retfail(ec, pilotErrorDiag)

        lfclfn = os.path.join(lfcdir, lfn)
        # LFC LFN = /grid/atlas/dq2/testpanda/testpanda.destDB.dfb45803-1251-43bb-8e7a-6ad2b6f205be_sub01000492/
        #364aeb74-8b62-4c8f-af43-47b447192ced_0.job.log.tgz

        # putfile is the SURL
        putfile = surl
        full_surl = putfile
        if full_surl[:len('token:')] == 'token:':
            # remove the space token (e.g. at Taiwan-LCG2) from the SURL info
            full_surl = full_surl[full_surl.index('srm://'):]

        # srm://dcache01.tier2.hep.manchester.ac.uk/pnfs/tier2.hep.manchester.ac.uk/data/atlas/dq2/
        #testpanda.destDB/testpanda.destDB.604b4fbc-dbe9-4b05-96bb-6beee0b99dee_sub0974647/
        #86ecb30d-7baa-49a8-9128-107cbfe4dd90_0.job.log.tgz
        tolog("putfile = %s" % (putfile))
        tolog("full_surl = %s" % (full_surl))

        # get the DQ2 site name from ToA
        try:
            _dq2SiteName = self.getDQ2SiteName(surl=putfile)
        except:
            # WARNING: do not print the exception here since it can sometimes not be converted to a string! (problem seen at Taiwan)
            tolog("Warning: Failed to get the DQ2 site name (can not add this info to tracing report)")
        else:
            report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
            tolog("DQ2 site name: %s" % (_dq2SiteName))

        # get the absolute (full) path to the file
        fppfn = os.path.abspath(pfn)
        tolog("pfn=%s" % (pfn))

        # cmd = '%s echo "LFC_HOST=$LFC_HOST"; lfc-mkdir -p %s' % (envsetup, lfcdir)
        # # export LFC_HOST=lfc0448.gridpp.rl.ac.uk ; echo "LFC_HOST=$LFC_HOST";
        # #lfc-mkdir -p /grid/atlas/dq2/testpanda.destDB/testpanda.destDB.604b4fbc-dbe9-4b05-96bb-6beee0b99dee_sub0974647
        # tolog("******* SAGA call to execute payload *********")
        # try:
        #     js = self.initSaga()
        #
        #     jd = saga.job.Description()
        #     # jd.project = self.project_id # should be taken from resourse description (pandaqueue)
        #     jd.wall_time_limit = 600
        #     jd.executable      = cmd
        #     jd.total_cpu_count = 1
        #     jd.queue = self.partition_comp
        #     jd.working_directory = self.remote_prefix
        #
        #     fork_job = js.create_job(jd)
        #     fork_job.add_callback(saga.STATE, self.jobStateChangeNotification)
        #
        #     #tolog("\n(PBS) Command: %s\n"  % to_script)
        #     fork_job.run()
        #     tolog("Local Job ID: %s" % fork_job.id)
        #
        #
        #     fork_job.wait()
        # except saga.SagaException, ex:
        #     # Catch all saga exceptions
        #     tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        #     # Trace back the exception. That can be helpful for debugging.
        #     tolog(" \n*** Backtrace:\n %s" % ex.traceback)

        # if s == 0:
        #     tolog("LFC setup and mkdir succeeded")
        #     tolog("Command output: %s" % (o))
        # else:
        #     tolog("!!WARNING!!2990!! LFC setup and mkdir failed. Status=%s Output=%s" % (s, o))
        #     if o == "Could not establish context":
        #         pilotErrorDiag = "Could not establish context: Proxy / VO extension of proxy has probably expired"
        #         tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
        #         self.dumpExtendedProxy(envsetup)
        #         self.__sendReport('CONTEXT_FAIL', report)
        #         return self.put_data_retfail(error.ERR_NOPROXY, pilotErrorDiag)
        #     else:
        #         pilotErrorDiag = "LFC setup and mkdir failed: %s" % (o)
        #         self.__sendReport('LFC_SETUP_FAIL', report)
        #         return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        # determine which timeout option to use
        timeout_option = "--srm-timeout=%d --connect-timeout=300 --sendreceive-timeout=%d" % (self.timeout, self.timeout)

        # used lcg-cr options:
        # --verbose: verbosity on
        #      --vo: specifies the Virtual Organization the user belongs to
        #        -T: specify SRM version
        #        -s: space token description
        #        -b: BDII disabling
        #        -t: time-out
        #        -l: specifies the Logical File Name associated with the file. If this option is present, an entry is added to the LFC
        #        -g: specifies the Grid Unique IDentifier. If this option is not present, a GUID is generated internally
        #        -d: specifies the destination. It can be the Storage Element fully qualified hostname or an SURL. In the latter case,
        #            the scheme can be sfn: for a classical SE or srm:. If only the fully qualified hostname is given, a filename is
        #            generated in the same format as with the Replica Manager
        if os.environ.has_key("LFC_HOST"):
            LFC_HOST_EN_VAR = os.environ['LFC_HOST']
        else:
            LFC_HOST_EN_VAR = readpar('lfchost')
        print "LFC_HOST_EN_VAR = %s" %LFC_HOST_EN_VAR
        if token:
            surl = putfile[putfile.index('srm://')]
            #_cmd_str = '%s echo "LFC_HOST=$LFC_HOST"; lfc-mkdir -p %s; which lcg-cr; lcg-cr --version; lcg-cr --verbose --vo atlas -T srmv2 -s %s -b %s -l %s -g %s -d %s file:%s' % (envsetup, lfcdir, token, timeout_option, lfclfn, guid, surl, fppfn)
            _cmd_str = '%s; %s export LFC_HOST=%s; lfc-mkdir -p %s; which lcg-cr; lcg-cr --version; lcg-cr --verbose --vo atlas -T srmv2 -s %s -b %s -l %s -g %s -d %s file:%s' % (source_cvmfs, envsetup, LFC_HOST_EN_VAR, lfcdir, token, timeout_option, lfclfn, guid, surl, fppfn)
        else:
            surl = putfile
            #_cmd_str = '%s echo "LFC_HOST=$LFC_HOST"; lfc-mkdir -p %s; which lcg-cr; lcg-cr --version; lcg-cr --verbose --vo atlas %s -l %s -g %s -d %s file:%s' % (envsetup, lfcdir, timeout_option, lfclfn, guid, surl, fppfn)
            _cmd_str = '%s; %s export LFC_HOST=%s; lfc-mkdir -p %s; which lcg-cr; lcg-cr --version; lcg-cr --verbose --vo atlas %s -l %s -g %s -d %s file:%s' % (source_cvmfs, envsetup, LFC_HOST_EN_VAR, lfcdir, timeout_option, lfclfn, guid, surl, fppfn)
            
        # GoeGrid testing: _cmd_str = '%s which lcg-cr; lcg-cr --version; lcg-crXXX --verbose --vo atlas %s -l %s -g %s -d %s file:%s' % (envsetup, timeout_option, lfclfn, guid, surl, fppfn)

        tolog("Executing command: %s" % (_cmd_str))
        s = -1
        t0 = os.times()
        report['relativeStart'] = time()
        report['transferStart'] =  time()
        o=""
        e=""
        epic.slurm(_cmd_str)
        o=epic.output
        e=epic.error
        s=epic.exit_code
        # tolog("******* SAGA call to stage-out *********")
        # try:
        #     js = self.initSaga()
        #
        #     #of=os.path.join(lfcdir, "so.out")
        #     #workDir = pdict.get('workDir', '')
        #     workDir = pfn.rsplit('/', 1)[0]
        #     #of=os.path.join(workDir, "so.out")
        #     of=workDir+"/so.out"
        #     #ef=os.path.join(lfcdir, "so.err")
        #     #ef=os.path.join(workDir, "so.err")
        #     ef=workDir+"/so.err"
        #
        #     jd = saga.job.Description()
        #     # jd.project = self.project_id # should be taken from resourse description (pandaqueue)
        #     jd.wall_time_limit = 600
        #     jd.executable      = _cmd_str
        #     jd.total_cpu_count = 1
        #     jd.queue = self.partition_comp
        #     jd.working_directory = self.remote_prefix
        #     jd.output=of
        #     jd.error=ef
        #
        #     fork_job = js.create_job(jd)
        #     # fork_job.add_callback(saga.STATE, self.jobStateChangeNotification)
        #
        #     #tolog("\n(PBS) Command: %s\n"  % to_script)
        #     fork_job.run()
        #     tolog("Local Job ID: %s" % fork_job.id)
        #
        #     fork_job.wait()
        #     # st=fork_job.state
        #     st = fork_job.get_state()
        #     # s=fork_job.exitcode
        #
        #     #lo=mktemp("out")
        #     #le=mktemp("err")
        #
        #     #fn="ssh://%s/%s" % (self.ssh_keypath,of)
        #
        #     fn="ssh://"+self.ssh_server+of
        #     tolog(fn)
        #     #file=saga.filesystem.File(fn,session=self.initSaga())
        #     #file=saga.filesystem.File(fn,self.__saga_session)
        #     file=saga.filesystem.File(fn)
        #     #file.copy("file://localhost/%s"%lo)
        #     file.copy("file://localhost/%s"%of)
        #     #with open (lo, "r") as myfile:
        #     with open (of, "r") as myfile:
        #         o=myfile.read()
        #
        #     #fn="ssh://%s/%s" % (self.ssh_keypath,ef)
        #     fn="ssh://" + self.ssh_server + ef
        #     #file=saga.filesystem.File(fn,session=self.initSaga())
        #     file=saga.filesystem.File(fn)
        #     #file.copy("file://localhost/%s"%le)
        #     file.copy("file://localhost/%s"%ef)
        #     #with open (lo, "r") as myfile:
        #     with open (ef, "r") as myfile:
        #         e=myfile.read()
        #
        #
        # except saga.SagaException, ex:
        #     # Catch all saga exceptions
        #     tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        #     # Trace back the exception. That can be helpful for debugging.
        #     tolog(" \n*** Backtrace:\n %s" % ex.traceback)
        #
        # (s,sts)={
        #     saga.job.NEW:(-4,"New"),
        #     saga.job.PENDING:(-3,"Pending"),
        #     saga.job.RUNNING:(-2,"Running"),
        #     saga.job.DONE:(0,"OK"),
        #     saga.job.FAILED:(1,"Failed"),
        #     saga.job.CANCELED:(2,"Canceled"),
        #     saga.job.SUSPENDED:(3,"Suspended")
        # }.get(st,(-1,"Unknown"))
        #
        #
        #
        # tolog("******* SAGA call ended with status %s *********"%sts)
        #tolog("******* SAGA call ended with status %s *********"%st)
        # tolog("Stdout:\n%s\n"%o)
        # tolog("Stderr:\n%s\n"%e)

        report['validateStart'] = time()
        t1 = os.times()
        t = t1[4] - t0[4]
        tolog("Command finished after %f s" % (t))
        tolog("exitcode = %d, len(stdout) = %s" % (s, len(o)))
        if s == 0:
            # add checksum and file size to LFC
            csumtype = self.getChecksumType(fchecksum, format="short")
            exitcode, pilotErrorDiag = self.addFileInfo(lfclfn, fchecksum, csumtype=csumtype)
            if exitcode != 0:
                # check if file was partially transferred, if so, remove it
                # note: --nolfc should not be used since lcg-cr succeeded in registering the file before
                ec = self.removeFile(envsetup, self.timeout, surl, nolfc=False, lfcdir=lfcdir)
                if ec == -2:
                    pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

            if exitcode == -1:
                self.__sendReport('LFC_IMPORT_FAIL', report)
                return self.put_data_retfail(error.ERR_PUTLFCIMPORT, pilotErrorDiag)
            elif exitcode != 0:
                self.__sendReport('LFC_ADDCSUM_FAIL', report)
                return self.put_data_retfail(error.ERR_LFCADDCSUMFAILED, pilotErrorDiag)
            else:
                tolog('Successfully set filesize and checksum for %s' % (pfn))
        else:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            check_syserr(s, o)
            tolog('!!WARNING!!2990!! put_data failed: exitcode = %d stdout = %s' % (s, o))

            # check if file was partially transferred, if so, remove it
            # note: --nolfc should only be used if LFC registration failed
            nolfc = self.useNoLFC(o)
            ec = self.removeFile(envsetup, self.timeout, surl, nolfc=nolfc, lfcdir=lfcdir)
            if ec == -2:
                pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

            if o.find("Could not establish context") >= 0:
                pilotErrorDiag += "Could not establish context: Proxy / VO extension of proxy has probably expired"
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.__sendReport('CONTEXT_FAIL', report)
                return self.put_data_retfail(error.ERR_NOPROXY, pilotErrorDiag)
            elif o.find("No such file or directory") >= 0:
                pilotErrorDiag += "No such file or directory: %s" % (o)
                self.__sendReport('NO_FILE_DIR', report)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
            elif o.find("globus_xio: System error") >= 0:
                pilotErrorDiag += "Globus system error: %s" % (o)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.__sendReport('GLOBUS_FAIL', report)
                return self.put_data_retfail(error.ERR_PUTGLOBUSSYSERR, pilotErrorDiag)
            else:
                if t >= self.timeout:
                    pilotErrorDiag += "Copy command self timed out after %d s" % (t)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    self.__sendReport('CP_TIMEOUT', report)
                    return self.put_data_retfail(error.ERR_PUTTIMEOUT, pilotErrorDiag)
                else:
                    if len(o) == 0:
                        pilotErrorDiag += "Copy command returned error code %d but no output" % (s)
                    else:
                        pilotErrorDiag += o
                    self.__sendReport('CP_ERROR', report)
                    return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        verified = False

        # get the checksum using the lcg-get-checksum command
        remote_checksum = self.lcgGetChecksum(envsetup, self.timeout, full_surl)

        if not remote_checksum:
            # try to grab the remote file info using lcg-ls command
            remote_checksum, remote_fsize = self.getRemoteFileInfo(envsetup, self.timeout, surl)
        else:
            tolog("Setting remote file size to None (not needed)")
            remote_fsize = None

        # compare the checksums if the remote checksum was extracted
        tolog("Remote checksum: %s" % str(remote_checksum))
        tolog("Local checksum: %s" % (fchecksum))

        if remote_checksum:
            if remote_checksum != fchecksum:
                # report corrupted file to consistency server
                # self.reportFileCorruption(full_surl)

                pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                 (csumtype, os.path.basename(dst_gpfn), remote_checksum, fchecksum)

                # remove the file and the LFC directory
                ec = self.removeFile(envsetup, self.timeout, surl, nolfc=False, lfcdir=lfcdir)
                if ec == -2:
                    pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

                tolog("!!WARNING!!1800!! %s" % (pilotErrorDiag))
                if csumtype == "adler32" or csumtype == "AD":
                    self.__sendReport('AD_MISMATCH', report)
                    return self.put_data_retfail(error.ERR_PUTADMISMATCH, pilotErrorDiag)
                else:
                    self.__sendReport('MD5_MISMATCH', report)
                    return self.put_data_retfail(error.ERR_PUTMD5MISMATCH, pilotErrorDiag)
            else:
                tolog("Remote and local checksums verified")
                verified = True
        else:
            tolog("Skipped primary checksum verification (remote checksum not known)")

        # if lcg-ls could not be used
        if "/pnfs/" in dst_gpfn and not remote_checksum:
            # for dCache systems we can test the checksum with the use method
            tolog("Detected dCache system: will verify local checksum with the local SE checksum")
            # gpfn = srm://head01.aglt2.org:8443/srm/managerv2?SFN=/pnfs/aglt2.org/atlasproddisk/mc08/EVNT/mc08.109270.J0....
            path = dst_gpfn[dst_gpfn.find('/pnfs/'):]
            # path = /pnfs/aglt2.org/atlasproddisk/mc08/EVNT/mc08.109270.J0....#
            tolog("File path: %s" % (path))

            _filename = os.path.basename(path)
            _dir = os.path.dirname(path)

            # get the remote checksum
            tolog("Local checksum: %s" % (fchecksum))
            try:
                remote_checksum = self.getdCacheChecksum(_dir, _filename)
            except Exception, e:
                pilotErrorDiag = "Could not get checksum from dCache: %s (test will be skipped)" % str(e)
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            else:
                if remote_checksum == "NOSUCHFILE":
                    pilotErrorDiag = "The pilot will fail the job since the remote file does not exist"

                    # remove the file and the LFC directory
                    ec = self.removeFile(envsetup, self.timeout, surl, nolfc=False, lfcdir=lfcdir)
                    if ec == -2:
                        pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    self.__sendReport('NOSUCHFILE', report)
                    return self.put_data_retfail(error.ERR_NOSUCHFILE, pilotErrorDiag)
                elif remote_checksum:
                    tolog("Remote checksum: %s" % (remote_checksum))
                else:
                    tolog("Could not get remote checksum")

            if remote_checksum:
                if remote_checksum != fchecksum:
                    # report corrupted file to consistency server
                    self.reportFileCorruption(full_surl)

                    pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                     (csumtype, _filename, remote_checksum, fchecksum)

                    # remove the file and the LFC directory
                    ec = self.removeFile(envsetup, self.timeout, surl, nolfc=False, lfcdir=lfcdir)
                    if ec == -2:
                        pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

                    if csumtype == "adler32":
                        self.__sendReport('AD_MISMATCH', report)
                        return self.put_data_retfail(error.ERR_PUTADMISMATCH, pilotErrorDiag)
                    else:
                        self.__sendReport('MD5_MISMATCH', report)
                        return self.put_data_retfail(error.ERR_PUTMD5MISMATCH, pilotErrorDiag)
                else:
                    tolog("Remote and local checksums verified")
                    verified = True
        else:
            tolog("Skipped secondary checksum test")

        # if the checksum could not be verified (as is the case for non-dCache sites) test the file size instead
        if not remote_checksum and remote_fsize:
            # compare remote and local file size
            tolog("Remote file size: %s" % str(remote_fsize))
            tolog("Local file size: %s" % (fsize))
            if remote_fsize and remote_fsize != "" and fsize != "" and fsize:
                if str(fsize) != remote_fsize:
                    # report corrupted file to consistency server
                    self.reportFileCorruption(full_surl)

                    pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                                     (filename, remote_fsize, str(fsize))

                    # remove the file and the LFC directory
                    ec = self.removeFile(envsetup, self.timeout, surl, nolfc=False, lfcdir=lfcdir)
                    if ec == -2:
                        pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    self.__sendReport('FS_MISMATCH', report)
                    return self.put_data_retfail(error.ERR_PUTWRONGSIZE, pilotErrorDiag)
                else:
                    tolog("Remote and local file sizes verified")
                    verified = True
            else:
                tolog("Skipped file size test")

        # was anything verified?

        #PN
        # if not '.log' in dst_gpfn:
        #     verified = False

        if not verified:
            # fail at this point
            pilotErrorDiag = "Neither checksum nor file size could be verified (failing job)"

            # remove the file and the LFC directory
            ec = self.removeFile(envsetup, self.timeout, surl, nolfc=False, lfcdir=lfcdir)
            if ec == -2:
                pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('NOFILEVERIFICATION', report)
            return self.put_data_retfail(error.ERR_NOFILEVERIFICATION, pilotErrorDiag)

        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag, dst_gpfn, 0, 0, self.arch_type

    def useNoLFC(self, _stdout):
        """ Should --nolfc be used? """

        if "Registration failed" in _stdout:
            nolfc = True
        else:
            nolfc = False
        return nolfc

    def __sendReport(self, state, report):
        """
        Send DQ2 tracing report. Set the client exit state and finish
        """
        if report.has_key('timeStart'):
            # finish instrumentation
            report['timeEnd'] = time()
            report['clientState'] = state
            # send report
            tolog("Updated tracing report: %s" % str(report))
            self.sendTrace(report)
