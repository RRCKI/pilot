#!/usr/bin/env python

"""Module to acccess the my_proxy and glexec services.

The MyProxyInterface class allows creating interfaces for my_proxy. The class
GlexecInterface allows the context switch to run a payload sandboxed.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import Configuration
import pUtil
import glob
import time
import epic
import shlex
import pipes

# Eddie
import re
import stat

from Monitor import Monitor
import environment
from SiteInformation import SiteInformation

pilot_remote_path="../../pilot_remote"

try:
    import simplejson as json
except ImportError:
    try:
        import json
    except ImportError:
        json = None

if json is not None:
    import CustomEncoder
else:
    CustomEncoder = None


def execute(program):
    """Run a program on the command line. Return stderr, stdout and status."""
    pUtil.tolog("executable: %s" % program)
    pipe = subprocess.Popen(program, bufsize=-1, shell=True, close_fds=False,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = pipe.communicate()
    return stdout, stderr, pipe.wait()


# TODO(rmedrano): These kind of generic exceptions must be refactored.
class GlexecException(Exception):
    """Exception to raise when interactions with glexec failed."""

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        return self.message

class RemoteMonitorStarter(object):
    """Starts Monitor somewhere abroad"""

    def __init__(self, payload='run.sh'):
        global pilot_remote_path
        """Initialize the wrapper with an optional payload.

        Payload is the final end-user command to be executed.
        It may contain also input options, for example:
        payload='athena --indataset=blah --outdataset=blah ...'
        """
        epic.init_epic()
        self.payload = payload
        self.output = None
        self.error = None
        self.status = None

        self.__unique_str=epic.unique_str()
        self.dirname="dir_"+self.__unique_str
        self.dirname_abs=os.path.join(epic.ssh_remote_path,self.dirname)
        self.__cmd_str=""
        self.__cmd("echo","Start of remote execution")
        # self.__cmd_append("chmod 0700 "+self.dirname)
        self.__cmd("cd",self.dirname)

        self.__cmd_append("cp -t ./ %s/*.py"%pilot_remote_path)
        self.__cmd_append("cp -Rt ./ %s/saga"%pilot_remote_path)
        self.__cmd_append("cp -Rt ./ %s/radical"%pilot_remote_path)
        self.__cmd_append("cp -Rt ./ %s/HPC"%pilot_remote_path)
        # try:# there can be either variant
        #     if shlex.quote(" '\"/\\")=="' '\"'\"'\"/\\'":
        #         self.__quote=lambda x: shlex.quote(x)
        #     else:
        #         self.__quote=lambda x: pipes.quote(x)
        # except Exception:
        #     self.__quote=lambda x: pipes.quote(x)

    def setup_and_run(self,corecount=1):
        """Prepare the environment and execute glexec."""
        self.__ship_queue_data()
        self.__ship_job_definition()
        # self.__dump_current_configuration('data-orig.json')
        self.__ship_cert()
        self.__setenv(corecount)
        self.__dump_current_configuration()
        self.__run(corecount)
        # self.__clean()

    def __cmd_append(self,cmd):
        self.__cmd_str+=cmd+"\n"

    def __cmd(self,*args):
        for a in args:
            # self.__cmd_str+=" "+self.__quote(a)
            self.__cmd_str+=" "+pipes.quote(a)
        self.__cmd_str+="\n"

    def __maybe_env(self,*args):
        for var in args:
            if os.environ.has_key(var):
                self.__cmd("export","%s=%s"%(var,os.environ[var]))

    def __setenv(self,corecount):
        # self.__cmd("export","PilotHomeDir="+os.environ['PilotHomeDir'])
        self.__cmd_append("export PilotHomeDir=`pwd`")
        self.__cmd("mkdir",self.dirname_abs+"/wd")
        cnf=Configuration.Configuration()
        cnf['thisSite'].workdir=self.dirname_abs+"/wd"
        self.__cmd("export","ATHENA_PROC_NUMBER=%s"%corecount)
        self.__cmd_append("export ATLAS_PYTHON_PILOT=`which python`")
        self.__maybe_env("RUCIO_AUTH_TYPE","RUCIO_HOME","RUCIO_ACCOUNT","VO_ATLAS_SW_DIR")
        self.__cmd_append("export PATH=/cvmfs/atlas.cern.ch/repo/sw/ddm/latest/DQ2Clients/opt/dq2/"+\
                          "bin/:/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest/bin/:$PATH")
        self.__cmd_append("export PYTHONPATH=/cvmfs/atlas.cern.ch/repo/sw/ddm/latest/DQ2Clients/opt"+\
                          "/dq2/lib:/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest/lib/"+\
                          "python2.6/site-packages:/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/"+\
                          "latest/externals/kerberos/lib.slc6-x86_64-2.6:/cvmfs/atlas.cern.ch/repo"+\
                          "/sw/ddm/rucio-clients/latest/externals/kerberos/lib.slc6-i686-2.6:"+\
                          "$PYTHONPATH")
        cnf['job'].datadir=self.dirname_abs

    def __extend_pythonpath(self):
        current_path = os.getcwd()
        pUtil.tolog('extending curring path to %s ' % current_path)
        sys.path.append(current_path)
        pUtil.tolog('sys path is ... %s ' % sys.path)

    def __dump_current_configuration(self, file_name='data.json'):
        """Dump the current configuration in JSON format into the sandbox"""
        pUtil.tolog('dumping config file...')
        if json is None:
            raise RuntimeError('json is not available')
        Configuration.Configuration()['SandBoxPath'] = self.dirname_abs
        CustomEncoder.ConfigurationSerializer.serialize_file(Configuration.Configuration(), file_name)
        pUtil.tolog('dumped config file at %s, moving to remote' %file_name)
        epic.push_file(file_name,self.dirname+'/')
        self.__cmd("chmod","0644",file_name)

    def __ship_queue_data(self):
        """Ships the queue data to the sandbox environment."""
        # Will now try to find if we have the json or dat extension and then copy it to the sandbox
        si = SiteInformation()
        queuedatafile = si.getQueuedataFileName()
        epic.push_file(queuedatafile,self.dirname+'/')
        self.__cmd_append("chmod 0666 queuedata.*")

        # for filename in glob.glob(os.path.join(os.environ['PilotHomeDir'], '*.py')):
        #         shutil.copy2(filename, self.sandbox_path)
        #         os.chmod(os.path.join(self.sandbox_path, filename), 0666)
        #
        # shutil.copytree(os.path.join(os.environ['PilotHomeDir'], 'saga'),
        #         os.path.join(self.sandbox_path, 'saga'))

        epic.push_file('../PILOTVERSION',self.dirname+'/PILOTVERSION')
        self.__cmd("chmod","0666","PILOTVERSION")

    def __ship_cert(self):
        """Ships the queue data to the sandbox environment."""
        # Will now try to find if we have the json or dat extension and then copy it to the sandbox
        epic.push_file(os.environ['X509_USER_PROXY'],self.dirname+'/x509.crt')
        self.__cmd("chmod","0600","x509.crt")
        self.__cmd("cp","x509.crt","x509_u.crt")
        self.__cmd("chmod","0644","x509_u.crt")
        self.__cmd_append("export X509_USER_PROXY=`pwd`/x509.crt")
        self.__cmd_append("export X509_USER_KEY=`pwd`/x509_u.crt")
        self.__cmd_append("export X509_USER_CERT=`pwd`/x509_u.crt")



    def __ship_job_definition(self):
        """Ships the job definition to the sandbox."""
        for filename in glob.glob("./Job_*.py"):
            epic.push_file(filename,self.dirname+'/')
        epic.push_file(Configuration.Configuration()['pandaJobDataFileName'],self.dirname+'/')
        # execute('cp ./Job_*.py %s' % self.sandbox_path)

    def __run(self,corecount):
        # """Start the sandboxed process for the job.
        #
        # See usage examples in:
        # http://wiki.nikhef.nl/grid/GLExec_Environment_Wrap_and_Unwrap_scripts
        # """
        # pUtil.tolog('correcting env vars before running glexec!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        # # Store actual workdir in order to restore it later on
        # env = Configuration.Configuration()
        # self.__actual_workdir = env['workdir']
        # env['workdir'] = self.sandbox_path + '/output'
        # if not os.path.exists(env['workdir']):
        #         os.makedirs(env['workdir'])
        #         os.chmod(env['workdir'],0777)
        # env['thisSite'].wntmpdir = env['workdir']
        # env['PilotHomeDir'] = self.sandbox_path
        # env['inputDir'] = self.sandbox_path
        # env['outputDir'] = self.sandbox_path
        # env['pilot_initdir'] = self.sandbox_path
        # pUtil.tolog('Pilot home dir is %s '%env['PilotHomeDir'])
        # self.__site_workdir = re.split('Panda_Pilot',env['thisSite'].workdir)
        # env['thisSite'].workdir = env['workdir'] + '/Panda_Pilot' + self.__site_workdir[1]
        # env['job'].datadir = env['thisSite'].workdir + '/PandaJob_' + env['job'].jobId + '_data'
        # if not os.path.exists(env['thisSite'].workdir):
        #         os.makedirs(env['thisSite'].workdir)
        #         os.chmod(env['thisSite'].workdir,0777)
        # self.__dump_current_configuration()
        #
        # pUtil.tolog("cding and running GLEXEC!")
        # cmd = "export GLEXEC_ENV=`%s`; \
        #        %s %s -- 'cd %s; \
        #        %s 2>1;'" % (self.__wrapper_path,
        #                      self.__glexec_path,
        #                      self.__unwrapper_path,
        #                      self.__target_path,
        #                      self.payload)
        # self.output, self.error, self.status = execute(cmd)
        # pUtil.tolog(self.error)
        # pUtil.tolog(self.output)
        cmd_str=self.__cmd_str+'source /adm/scripts/grid/emi-wn-cvmfs-environment.sh\n'\
                +"export PYTHONPATH=$PYTHONPATH:/s/ls2/groups/g0037/pilot_remote\n"\
                +"echo ls -la `pwd`;ls -la ;echo ls -la `pwd`/..;ls -la ..;echo ------------;env;echo ------------\n"\
                +self.payload
        epic.slurm(cmd_str,corecount)
        self.output=epic.output
        self.error=epic.error
        self.status=epic.exit_code

    def __clean(self):
        epic.delete(self.dirname)


if __name__ == "__main__":
    jid=epic.slurm('ls -la;sleep 30;hostname>&2',nonblocking=True,walltime=100)
    print("SLURM JID=%d"%jid)
    epic.slurm_wait_queued(jid)
    print("started")
    epic.slurm_wait(jid)
    print("ended")
    # epic.state_parse('JobId=523525 Name=hfb4_run UserId=saper(1225) GroupId=g0051(1051) Priority=14000000 Nice=0 Account=g0051 QOS=ki JobState=RUNNING Reason=None Dependency=(null) Requeue=0 Restarts=0 BatchFlag=1 ExitCode=0:0 DerivedExitCode=0:0 RunTime=00:02:56 TimeLimit=03:00:00 TimeMin=N/A SubmitTime=2015-05-05T14:49:55 EligibleTime=2015-05-05T14:49:55 StartTime=2015-05-05T14:49:58 EndTime=2015-05-05T17:49:58 PreemptTime=None SuspendTime=None SecsPreSuspend=0 Partition=hpc2-16g-3d AllocNode:Sid=weed1:30204 ReqNodeList=(null) ExcNodeList=(null) NodeList=cn1084 BatchHost=cn1084 NumNodes=1 NumCPUs=1 CPUs/Task=1 ReqB:S:C:T=0:0:*:* Socks/Node=* NtasksPerN:B:S:C=0:0:*:* CoreSpec=65534   Nodes=cn1084 CPU_IDs=2 Mem=0 MinCPUsNode=1 MinMemoryNode=0 MinTmpDiskNode=0 Features=(null) Gres=(null) Reservation=(null) Shared=OK Contiguous=0 Licenses=(null) Network=(null) Command=/s/ls2/users/saper/HFB/hfb4/hfb4_run WorkDir=/s/ls2/users/saper/HFB/hfb4 StdErr=/s/ls2/users/saper/HFB/hfb4/%j.err StdIn=/dev/null StdOut=/s/ls2/users/saper/HFB/hfb4/523525.out')
    quit()
