"""
This is Exec Pilot In Cloud (EPIC) for SAGA
Author: Daniel Drizhuk (d.drizhuk@gmail.com)

This is simple yet handy way to push and pull files from a remote PC using SAGA
It is also a way to run tasks and obtain results without a pain in a butt
"""


import saga
import os
import commands
import pipes
import shutil
import subprocess
import sys
import tempfile
import Configuration
import pUtil
import glob
import time
import csv
import errno
import re
import hpcconf
import datetime
import random
from simpleflock import SimpleFlock

lock_fn='~/.ssh/epic.lockfile'

saga_context = None
saga_session = None

queue=hpcconf.queue

ssh_user=hpcconf.ssh.user
ssh_pass=hpcconf.ssh.passwd
ssh_keypath=hpcconf.ssh.keypath
ssh_server=hpcconf.ssh.server
ssh_remote_home=hpcconf.ssh.remote_home
ssh_remote_temp=hpcconf.ssh.remote_temp
ssh_remote_path=None

__jobs={}

__unique_str_counter=0

output=""
error=""
state="Undefined"
exit_code=-1

job_wait_pending=5 #min
job_wait_time=30 #sec


random.seed()
session=random.randint(0,7)
# session=1

lock_timeout=300
lock_fn+=".%d"%session

class NakedObject(object):
    pass

def __R(path,lfn=False):
    global ssh_remote_home,ssh_remote_path
    if path=="~":
        path=ssh_remote_home+"/"
    elif path[0:2]=="~/":
        path.replace('~', ssh_remote_home, 1)

    if type(lfn) is str and path[-1:]=='/':
        path=os.path.join(path,lfn)

    if path[0]!='/':
        path=os.path.join(ssh_remote_path,path)

    path=os.path.normpath(path)
    (dn,fn)=os.path.split(path)
    return path,dn,fn

def __COE():
    global ssh_remote_temp
    uniqueStr=unique_str()
    outfnp=os.path.join(ssh_remote_temp,"out_%s"%uniqueStr)
    cmd_file=os.path.join(ssh_remote_temp,"cmd_%s.sh"%uniqueStr)
    return cmd_file,"%s.out"%outfnp,"%s.err"%outfnp

def __L(path,rfn=False):
    if type(rfn) is str and path[-1:]=='/':
        path=os.path.join(path,rfn)
    if path[0]!='/':
        path=os.path.abspath(path)

    path=os.path.normpath(path)
    (dn,fn)=os.path.split(path)
    return path,dn,fn

def init_epic(server=False,user=False,password=False,keypath=False,home=False,temp=False):
    global ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,ssh_remote_home,ssh_remote_temp,ssh_server
    if (type(home) is str) and os.path.isabs(home):
        ssh_remote_home=os.path.normpath(home)
    if (type(temp) is str) and os.path.isabs(temp):
        ssh_remote_temp=os.path.normpath(temp)
    if type(server) is str and server!=ssh_server:
        ssh_server=server
    if type(user) is str and user!=ssh_user:
        ssh_user=user
    if type(password) is str and password!=ssh_pass:
        ssh_pass=password
    if type(keypath) is str and keypath!=ssh_keypath:
        ssh_keypath=keypath

    if type(ssh_remote_path) is not str:
        ssh_remote_path=ssh_remote_home

def ssh_ident():
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,ssh_remote_home,ssh_remote_temp,ssh_server
    usr=ssh_user
    if type(ssh_pass) is str and ssh_pass!='':
        usr=usr+':'+ssh_pass

    return usr+"@"+ssh_server


def ssh_command(with_ident=True):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,ssh_remote_home,ssh_remote_temp,ssh_server
    usr=ssh_user
    if type(ssh_pass) is str and ssh_pass!='':
        usr=usr+':'+ssh_pass

    cmd="ssh"

    # cmd+=" -o \"ControlMaster auto\" -S \"~/.ssh/controlmasters/"+ssh_ident()+("_%d"%session)+"\" -o \"ControlPersist 10m\""
    if with_ident:
        cmd+=" "+pipes.quote(ssh_ident())

    if type(ssh_keypath) is str and ssh_keypath!='':
        cmd+=" -i "+pipes.quote(ssh_keypath)
    return cmd

def unique_str():
    global __unique_str_counter
    __unique_str_counter+=1
    t="%s"%int(round(time.time()*1000))
    return "%s_%s%s"%(os.getpid(),t[-7:],__unique_str_counter)

def ssh(cmd):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,error,output,state,exit_code,ssh_remote_home
    pUtil.tolog("*********EPIC*********")
    pUtil.tolog("Executing external command: %s"%cmd)
    init_epic()

    sshcmd=ssh_command()

    cmd_file,out_fn,err_fn=__COE()
    cmd = 'export HOME=' + pipes.quote(ssh_remote_home) +\
          '\ncd ' +pipes.quote(ssh_remote_path)+\
          '\nsh -c ' + pipes.quote(cmd) + ' >'+pipes.quote(out_fn) + ' 2>'+pipes.quote(err_fn)+\
          '\nexit $?'

    # print(sshcmd+' '+pipes.quote(cmd))
    with SimpleFlock(lock_fn,lock_timeout):
        exit_code,o=commands.getstatusoutput(sshcmd+' '+pipes.quote(cmd))
    exit_code,exit_code_ssh=divmod(exit_code,256)
    pUtil.tolog("Exit code: %s"%exit_code)
    # pUtil.tolog("Exit code: %s, %s, %s"%(exit_code,exit_code_ssh,o))

    output=read(out_fn,True)
    error=read(err_fn,True)
    pUtil.tolog("Output file:\n"+\
                "-----------------------------------------------------------------------------------------------------\n"+\
                output+\
                "\n-----------------------------------------------------------------------------------------------------")
    pUtil.tolog("Error file:\n"+\
                "-----------------------------------------------------------------------------------------------------\n"+\
                error+\
                "\n-----------------------------------------------------------------------------------------------------")

    pUtil.tolog("*********END**********")

class JobInfo(object):
    failcounter=0
    failcounter_raised=0

    def __init__(self,scontrol):
        if isinstance(scontrol, basestring):
            self.failcounter_raised = 0
            self.__str=scontrol
            if self.wrongId():
                self.state='WRONG_ID'
            else:
                self.state=self.__state()
        else:
            pUtil.tolog("Wrong JobInfo value %s"% scontrol)
            self.failcounter += 1
            self.failcounter_raised = 1

    def wrongId(self):
        if 'Invalid job id' in self.__str:
            return True
        return False

    def se(self,needle):
        try:
            ret=re.search('(?<=\s%s=)\S*'%needle,self.__str).group(0)
            self.failcounter_raised = 0
            self.failcounter = 0
        except:
            pUtil.tolog("Can not parse job state: %s"% self.__str)
            ret=""
            self.failcounter += 1
            self.failcounter_raised = 1
            if self.failcounter >10:
                raise
        return ret

    def ec(self):
        if self.wrongId():
            return -1
        return long(self.se('ExitCode').split(':')[0])

    def __state(self):
        return self.se('JobState')

    def state_is_final(state):
        if state in ['CANCELLED','COMPLETED','FAILED','NODE_FAIL','PREEMPTED','TIMEOUT','WRONG_ID']:
            return True
        return False
    state_is_final = staticmethod(state_is_final)

    def state_test_runtime(state):
        if state in ['COMPLETING']:
            return True
        return False

    def state_no_output(state):
        if state in ['CANCELLED','TIMEOUT','WRONG_ID','PENDING']:
            return True
        return False
    state_no_output=staticmethod(state_no_output)

    def is_final(self):
        return JobInfo.state_is_final(self.state)

    def has_output(self):
        return not JobInfo.state_no_output(self.state)

def slurm(cmd,cpucount=1,walltime=10000,nonblocking=False,wait_queued=0): # 10000 min ~= 1 week
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,queue,error,output,state,exit_code,job_wait_pending,job_wait_time,ssh_remote_home,__jobs
    pUtil.tolog("*********EPIC*********")
    pUtil.tolog("Executing external command: %s"%cmd)
    init_epic()
    job=NakedObject()

    hours   = walltime / 60
    minutes = walltime % 60

    job.cmd_file,job.out_fn,job.err_fn=__COE()
    cmd = '#!/bin/sh\n#SBATCH -o '+pipes.quote(job.out_fn)+'\n'+\
          '#SBATCH -e '+pipes.quote(job.err_fn)+'\n'+\
          '#SBATCH -D '+pipes.quote(ssh_remote_path)+'\n'+\
          ('#SBATCH -n %d\n'%long(cpucount))+\
          '#SBATCH -p '+pipes.quote(queue)+'\n'+\
          ('#SBATCH -t %02d:%02d:00\n'%(long(hours),long(minutes)))+\
          cmd
          
    #pUtil.tolog('EPIC executing script: %s'%cmd)
    write(job.cmd_file,cmd)

    sshcmd='export HOME=' + pipes.quote(ssh_remote_home) +';sbatch '+pipes.quote(job.cmd_file)

    with SimpleFlock(lock_fn,lock_timeout):
        e,o=commands.getstatusoutput(ssh_command()+' '+pipes.quote(sshcmd))
    o1=o
    o=o.split(' ')
    job.jid=-1
    if o[0]=='Submitted' and o[2]=='job':
        job.jid=long(o[3])

    if job.jid < 0:
        pUtil.tolog("Error of submiting job to slurm: %s"%o1)
        return job.jid

    pUtil.tolog("Job id: %d"  %  job.jid)

    job.walltime=walltime
    job.endtime=False
    job.time_waisted=0.
    job.cancelling=False
    job.old_state=-4
    job.waiting=False
    job.wait_time=wait_queued

    __jobs[job.jid]=job

    slurm_status(job.jid)
    if job.info.is_final():
        slurm_finalize(job.jid)

    if not nonblocking:
        return slurm_wait(job.jid)

    return job.jid

def slurm_status(jid):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,queue,error,output,state,exit_code,job_wait_pending,job_wait_time,ssh_remote_home,__jobs
    test_forgiveness=jid
    test_forgiveness+=1 #test forgiveness for integers should break here

    job=__jobs[jid]
    assert job.jid==jid

    scontrolcmd='scontrol -od show job %d'%job.jid
    sshcmd=ssh_command()+' '+pipes.quote(scontrolcmd)

    with SimpleFlock(lock_fn,lock_timeout):
        e,o=commands.getstatusoutput(sshcmd)
    job.info=JobInfo(o)
    return job.info

def slurm_job_queue_walltime_exceded(jid):
    job=__jobs[jid]
    assert job.jid==jid
    return job.cancelling

def slurm_wait_queued(jid):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,queue,error,output,state,exit_code,job_wait_pending,job_wait_time,ssh_remote_home,__jobs
    test_forgiveness=jid
    test_forgiveness+=1 #test forgiveness for integers should break here

    job=__jobs[jid]
    assert job.jid==jid

    try:
        job.wait_time+=1
        job.wait_time-=1
    except TypeError:
        job.wait_time=job_wait_pending

    if not job.info.is_final() and not job.waiting:
        pUtil.tolog("Waiting in queue")
        pUtil.tolog("Wait time: %d"%job.wait_time)
        while True:
            st=slurm_get_state(jid)

            if st=='RUNNING' and not job.waiting or job.info.is_final():
                job.endtime=datetime.datetime.strptime(job.info.se("StartTime"),"%Y-%m-%dT%H:%M:%S")+datetime.timedelta(0,job.walltime*60)
                job.waiting=True
                break

            if job.time_waisted>(job.wait_time*60)>0 and not job.waiting:
                pUtil.tolog("Job is pending for too long, aborting")

                with SimpleFlock(lock_fn,lock_timeout):
                    e,o=commands.getstatusoutput(ssh_command()+' '+pipes.quote('scancel %d'%job.jid))
                job.cancelling=True
                job.waiting=True
                break

            time.sleep(job_wait_time)
            job.time_waisted+=job_wait_time

def slurm_finalize(jid):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,queue,error,output,state,exit_code,job_wait_pending,job_wait_time,ssh_remote_home,__jobs
    test_forgiveness=jid
    test_forgiveness+=1 #test forgiveness for integers should break here

    job=__jobs[jid]
    assert job.jid==jid

    assert job.info.is_final()

    pUtil.tolog("Job ended with state %s"%job.info.state)
    pUtil.tolog("Exit code: %s"%job.info.ec())

    if job.info.state=='WRONG_ID':
        job.output=''
        job.error='Wrong SLURM job ID returned'
        delete(job.out_fn)
        delete(job.err_fn)
    else:

        if job.info.has_output():
            job.output=read(job.out_fn,True)
            job.error=read(job.err_fn,True)
        else:
            job.output=""
            job.error=job.info.state
            delete(job.out_fn)
            delete(job.err_fn)


        pUtil.tolog("Output file:\n"+\
                    "-----------------------------------------------------------------------------------------------------\n"+\
                    job.output+\
                    "\n-----------------------------------------------------------------------------------------------------")
        pUtil.tolog("Error file:\n"+\
                    "-----------------------------------------------------------------------------------------------------\n"+\
                    job.error+\
                    "\n-----------------------------------------------------------------------------------------------------")

    delete(job.cmd_file)

    if job.cancelling:
        exit_code=-100

    pUtil.tolog("*********END**********")

def slurm_get_state(jid):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,queue,error,output,state,exit_code,job_wait_pending,job_wait_time,ssh_remote_home,__jobs
    test_forgiveness=jid
    test_forgiveness+=1 #test forgiveness for integers should break here

    job=__jobs[jid]
    assert job.jid==jid

    scontrolcmd='scontrol -od show job %d'%job.jid
    sshcmd=ssh_command()+' '+pipes.quote(scontrolcmd)

    if not job.info.is_final():
        with SimpleFlock(lock_fn,lock_timeout):
            e,o=commands.getstatusoutput(sshcmd)
        st=''
        try:
            jd=JobInfo(o)
            st=jd.state
            if st!=job.info.state:
                pUtil.tolog("Job state changed to %s"  %  st)

            job.info=jd

            if isinstance(job.endtime,datetime.datetime) and job.endtime<datetime.datetime.now():
                pUtil.tolog("Job exceeded walltime, cancelling")
                with SimpleFlock(lock_fn,lock_timeout):
                    e,o=commands.getstatusoutput(ssh_command()+' '+pipes.quote('scancel %d'%job.jid))
                job.cancelling=True
                job.info.state='CANCELLED'

            if job.info.is_final():
                slurm_finalize(jid)


            return job.info.state
        except:
            pUtil.tolog("SLURM returned:(%s) %s"  %  (e,o))
            return 'FAIL_TO_DETERMINE_STATE'



def slurm_wait(jid):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,queue,error,output,state,exit_code,job_wait_pending,job_wait_time,ssh_remote_home,__jobs
    test_forgiveness=jid
    test_forgiveness+=1 #test forgiveness for integers should break here

    job=__jobs[jid]
    assert job.jid==jid

    slurm_wait_queued(jid)

    scontrolcmd='scontrol -od show job %d'%job.jid
    sshcmd=ssh_command()+' '+pipes.quote(scontrolcmd)

    if not job.info.is_final():
        pUtil.tolog("Waiting for job to end")
        while True:
            slurm_get_state(jid)

            if job.info.is_final():
                break

            time.sleep(job_wait_time)

    state=job.info.state
    exit_code=job.info.ec()
    output=job.output
    error=job.error

def fetch_file(original,local='./',remove=False):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,ssh_remote_home
    init_epic()

    original,rdn,rfn=__R(original)
    local,ldn,lfn=__L(local,rfn)

    cmd="rsync -e "+pipes.quote(ssh_command(False))+' -rtpL '
    if remove:
        cmd+='--remove-sent-files --remove-source-files '

    s,o=commands.getstatusoutput('mkdir -p '+pipes.quote(ldn))

    cmd+=pipes.quote(ssh_ident()+":"+original)+" "+pipes.quote(local)

    # print(cmd)
    with SimpleFlock(lock_fn,lock_timeout):
        s, o = commands.getstatusoutput(cmd)

    # pUtil.tolog("rsync returned %d: %s"%(s,o))

    return local

def cd(dir="~"):
    global ssh_remote_path
    init_epic()

    ssh_remote_path,x,y=__R(dir)
    return ssh_remote_path

def ls(dir=".",extended=False):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path
    dir,x,y=__R(dir)
    # du="ssh://%s%s"%(ssh_server,dir)
    # fp = saga.filesystem.Directory(du, session=saga_session)
    # list=fp.list()
    # for f in list:
    #     ret.append(str(f))
    # fp.close()
    cmd="rsync -e "+pipes.quote(ssh_command(False))+" --list-only "+pipes.quote(ssh_ident()+":"+dir+"/")
    # print(cmd)
    with SimpleFlock(lock_fn,lock_timeout):
        s, o = commands.getstatusoutput(cmd)
    reader = csv.DictReader(o.decode('ascii').splitlines(),
                        delimiter=' ', skipinitialspace=True,
                        fieldnames=['permissions', 'size',
                                    'date', 'time', 'name'])
    if extended:
        return reader
    ret=[]
    for f in reader:
        if f['name'] != '.':
            ret.append(f['name'])
    return ret

def delete(remote):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path
    init_epic()

    remote,rdn,rfn=__R(remote)

    pUtil.tolog("Deleting remote file: %s"%remote)
    cmd=ssh_command()+" rm -rf "+pipes.quote(remote)
    with SimpleFlock(lock_fn,lock_timeout):
        s, o = commands.getstatusoutput(cmd)

def push_file(original,remote='./'):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path
    init_epic()

    original,odn,ofn=__L(original)
    remote,rdn,rfn=__R(remote,ofn)
    mkdir='mkdir -p '+pipes.quote(rdn)+'/ && rsync'

    cmd="rsync -e "+pipes.quote(ssh_command(False))+' --rsync-path='+pipes.quote(mkdir)+' -rtpL '

    cmd+=pipes.quote(original)+" "+pipes.quote(ssh_ident()+":"+remote)

    # print(cmd)
    with SimpleFlock(lock_fn,lock_timeout):
        s, o = commands.getstatusoutput(cmd)

    return remote

def read(original,delete=False):
    tmpname=mymktemp()
    pUtil.tolog("Using temporary name: %s"%tmpname)
    fetch_file(original,tmpname,delete)
    pUtil.tolog("Reading tmp file")
    ret=open(tmpname).read()
    pUtil.tolog("Removing tmp file")
    os.remove(tmpname)
    return ret

def write(filename,str,append=False):
    tmpname=mymktemp()
    pUtil.tolog("Using temporary name: %s"%tmpname)
    if append:
        prepend=read(filename)
    f=open(tmpname,"w+")
    if append:
        f.write(prepend)
    f.write(str)
    f.close()
    ret=push_file(tmpname,filename)
    pUtil.tolog("Removing tmp file")
    os.remove(tmpname)
    return ret

def mymktemp():
    tmpname="./tempfile"+unique_str()
    return tmpname

