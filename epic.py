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
import re
import hpcconf

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
job_wait_time=10 #sec
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

    exit_code,o=commands.getstatusoutput(sshcmd+' '+pipes.quote(cmd))
    exit_code,exit_code_ssh=divmod(exit_code,256)
    pUtil.tolog("Exit code: %s"%exit_code)

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
    def __init__(self,scontrol):
        self.__str=scontrol
        if self.wrongId():
            self.state='WRONG_ID'
        else:
            self.state=self.__state()

    def wrongId(self):
        if 'Invalid job id' in self.__str:
            return True
        return False

    def se(self,needle):
        return re.search('(?<=\s%s=)\S*'%needle,self.__str).group(0)

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

    def is_final(self):
        return JobInfo.state_is_final(self.state)

def slurm(cmd,cpucount=1,walltime=10000,nonblocking=False): # 10000 min ~= 1 week
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
    write(job.cmd_file,cmd)

    sshcmd='export HOME=' + pipes.quote(ssh_remote_home) +';sbatch '+pipes.quote(job.cmd_file)

    e,o=commands.getstatusoutput(ssh_command()+' '+pipes.quote(sshcmd))
    o=o.split(' ')
    job.jid=-1
    if o[0]=='Submitted' and o[2]=='job':
        job.jid=long(o[3])
    pUtil.tolog("Job id: %d"  %  job.jid)

    job.time_waisted=0
    job.old_state=-4
    job.waiting=False

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

    e,o=commands.getstatusoutput(sshcmd)
    job.info=JobInfo(o)
    return job.info

def slurm_wait_queued(jid,wait_time=False):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,queue,error,output,state,exit_code,job_wait_pending,job_wait_time,ssh_remote_home,__jobs
    test_forgiveness=jid
    test_forgiveness+=1 #test forgiveness for integers should break here

    job=__jobs[jid]
    assert job.jid==jid

    try:
        wait_time+=1
        wait_time-=1
    except TypeError:
        wait_time=job_wait_pending


    scontrolcmd='scontrol -od show job %d'%job.jid
    sshcmd=ssh_command()+' '+pipes.quote(scontrolcmd)


    if not job.info.is_final() and not job.waiting:
        pUtil.tolog("Waiting in queue")
        while True:
            e,o=commands.getstatusoutput(sshcmd)
            jd=JobInfo(o)

            st=jd.state

            if st!=job.info.state:
                pUtil.tolog("Job state changed to %s"  %  st)
            job.info=jd

            if st=='RUNNING' and not job.waiting:
                job.waiting=True
                break

            if job.time_waisted>wait_time>0 and not job.waiting:
                pUtil.tolog("Job is pending for too long, aborting")
                e,o=commands.getstatusoutput(ssh_command()+' '+pipes.quote('scancel %d'%job.jid))
                job.waiting=True
                break

            if job.info.is_final():
                slurm_finalize(jid)
                break

            time.sleep(job_wait_time)
            job.time_waisted+=job_wait_time/60

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

        job.output=read(job.out_fn,True)
        job.error=read(job.err_fn,True)
        pUtil.tolog("Output file:\n"+\
                    "-----------------------------------------------------------------------------------------------------\n"+\
                    job.output+\
                    "\n-----------------------------------------------------------------------------------------------------")
        pUtil.tolog("Error file:\n"+\
                    "-----------------------------------------------------------------------------------------------------\n"+\
                    job.error+\
                    "\n-----------------------------------------------------------------------------------------------------")

    delete(job.cmd_file)

    pUtil.tolog("*********END**********")

def slurm_wait(jid):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,queue,error,output,state,exit_code,job_wait_pending,job_wait_time,ssh_remote_home,__jobs
    test_forgiveness=jid
    test_forgiveness+=1 #test forgiveness for integers should break here

    job=__jobs[jid]
    assert job.jid==jid

    slurm_wait_queued(jid,0)

    scontrolcmd='scontrol -od show job %d'%job.jid
    sshcmd=ssh_command()+' '+pipes.quote(scontrolcmd)

    if not job.info.is_final():
        pUtil.tolog("Waiting for job to end")
        while True:
            e,o=commands.getstatusoutput(sshcmd)
            jd=JobInfo(o)

            st=jd.state

            if st!=job.info.state:
                pUtil.tolog("Job state changed to %s"  %  st)
            job.info=jd

            if job.info.is_final():
                break

            time.sleep(job_wait_time)

        slurm_finalize(jid)
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

    print(cmd)
    s, o = commands.getstatusoutput(cmd)

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
    print(cmd)
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
    s, o = commands.getstatusoutput(cmd)

def push_file(original,remote='./'):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path
    init_epic()

    original,odn,ofn=__L(original)
    remote,rdn,rfn=__R(remote,ofn)
    mkdir='mkdir -p '+pipes.quote(rdn)+'/ && rsync'

    cmd="rsync -e "+pipes.quote(ssh_command(False))+' --rsync-path='+pipes.quote(mkdir)+' -rtpL '

    cmd+=pipes.quote(original)+" "+pipes.quote(ssh_ident()+":"+remote)

    print(cmd)
    s, o = commands.getstatusoutput(cmd)

    return remote

def read(original,delete=False):
    tmpname=tempfile.mktemp()
    pUtil.tolog("Using temporary name: %s"%tmpname)
    fetch_file(original,tmpname,delete)
    pUtil.tolog("Reading tmp file")
    ret=open(tmpname).read()
    pUtil.tolog("Removing tmp file")
    os.remove(tmpname)
    return ret

def write(filename,str,append=False):
    tmpname=tempfile.mktemp()
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
