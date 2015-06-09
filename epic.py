"""
This is Exec Pilot In Cloud (EPIC) for SAGA
Author: Daniel Drizhuk (d.drizhuk@gmail.com)

This is simple yet handy way to push and pull files from a remote PC using SAGA
It is also a way to run tasks and obtain results without a pain in a butt
"""


import saga
import os
import shutil
import subprocess
import sys
import tempfile
import Configuration
import pUtil
import glob
import time

saga_context = None
saga_session = None

queue="bamboo-1w"

ssh_user="poyda"
ssh_pass=""
ssh_keypath="/home/apf/.ssh/sk_poyda_rsa"
ssh_server="ui2.computing.kiae.ru"
ssh_remote_home="/s/ls2/groups/g0037/panda"
ssh_remote_temp="/s/ls2/groups/g0037/tmp"
ssh_remote_path=None

__unique_str_counter=0

output=""
error=""
state="Undefined"
exit_code=-1

job_wait_pending=5 #min
job_wait_time=30 #sec

def __R(path,lfn=False):
    global ssh_remote_home,ssh_remote_path
    if path=="~":
        path=ssh_remote_home+"/"
    elif path[0:2]=="~/":
        path.replace('~', ssh_remote_home, 1)

    if path[0]!='/':
        path=os.path.join(ssh_remote_path,path)

    if type(lfn) is str and path[-1:]=='/':
        path=os.path.join(path,lfn)

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
    if path[0]!='/':
        path=os.path.abspath(path)
    if type(rfn) is str and path[-1:]=='/':
        path=os.path.join(path,rfn)

    path=os.path.normpath(path)
    (dn,fn)=os.path.split(path)
    return path,dn,fn

def init_saga(server=False,user=False,password=False,keypath=False,home=False,temp=False):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,ssh_remote_home,ssh_remote_temp,ssh_server
    changed=False
    if (type(home) is str) and os.path.isabs(home):
        ssh_remote_home=os.path.normpath(home)
    if (type(temp) is str) and os.path.isabs(temp):
        ssh_remote_temp=os.path.normpath(temp)
    if type(server) is str and server!=ssh_server:
        ssh_server=server
        changed=True
    if type(user) is str and user!=ssh_user:
        ssh_user=user
        changed=True
    if type(password) is str and password!=ssh_pass:
        ssh_pass=password
        changed=True
    if type(keypath) is str and keypath!=ssh_keypath:
        ssh_keypath=keypath
        changed=True

    if changed:
        saga_session.remove_context(saga_context)
        saga_session=None

    if not saga_session:
        pUtil.tolog("*********SAGA INIT*********")
        try:
            ssh_remote_path=ssh_remote_home
            saga_context=saga.Context("ssh")
            saga_context.user_id   = ssh_user
            saga_context.user_cert = ssh_keypath # private key derived from cert
            saga_context.user_pass = ssh_pass

            saga_session = saga.Session()
            saga_session.add_context(saga_context)

        except saga.SagaException, ex:
            # Catch all saga exceptions
            pUtil.tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
            # Trace back the exception. That can be helpful for debugging.
            pUtil.tolog(" \n*** Backtrace:\n %s" % ex.traceback)
            saga_session=None

    return saga_session

def unique_str():
    global __unique_str_counter
    __unique_str_counter+=1
    t="%s"%int(round(time.time()*1000))
    return "%s_%s%s"%(os.getpid(),t[-7:],__unique_str_counter)

def ssh(cmd):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,error,output,state,exit_code,ssh_remote_home
    pUtil.tolog("*********SAGA*********")
    pUtil.tolog("Executing external command: %s"%cmd)

    init_saga()
    try:
        jd=saga.job.Description()
        jd.executable="/bin/sh"
        jd.environment={'HOME':ssh_remote_home}
        cmd_file,out_fn,err_fn=__COE()
        jd.arguments=[cmd_file]
        jd.output=out_fn
        jd.error=err_fn
        jd.working_directory=ssh_remote_path

        write(cmd_file,cmd)

        pUtil.tolog("SAGA service init")
        js=saga.job.Service("ssh://%s" % ssh_server,
                                  session=saga_session)
        pUtil.tolog("SAGA job init")
        job=js.create_job(jd)
        pUtil.tolog("SAGA Job run")
        job.run()
        job.wait()
        state={
            saga.job.NEW:"New",
            saga.job.PENDING:"Pending",
            saga.job.RUNNING:"Running",
            saga.job.DONE:"OK",
            saga.job.FAILED:"Failed",
            saga.job.CANCELED:"Canceled",
            saga.job.SUSPENDED:"Suspended"
        }.get(job.state,"Unknown")
        pUtil.tolog("SAGA Job ended with state %s"%state)
        try:
            exit_code=job.exit_code
        except:
            exit_code=-1
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

        delete(cmd_file)

    except saga.SagaException, ex:
        # Catch all saga exceptions
        pUtil.tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        pUtil.tolog(" \n*** Backtrace:\n %s" % ex.traceback)

    pUtil.tolog("*********END**********")

def __state_monitor(self, source, metric, value):
    pUtil.tolog("Job %s state changed to %s : %s"  % (source, value,self.state_detail))

def slurm(cmd,cpucount=1,walltime=10000): # 10000 min ~= 1 week
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,queue,error,output,state,exit_code,job_wait_pending,job_wait_time,ssh_remote_home
    pUtil.tolog("*********SAGA*********")
    pUtil.tolog("Executing external command: %s"%cmd)

    init_saga()
    try:
        jd=saga.job.Description()
        jd.executable="/bin/sh"
        jd.environment={'HOME':ssh_remote_home}
        jd.queue=queue
        jd.total_cpu_count=cpucount
        jd.wall_time_limit=walltime
        cmd_file,out_fn,err_fn=__COE()
        jd.arguments=[cmd_file]
        jd.output=out_fn
        jd.error=err_fn
        jd.working_directory=ssh_remote_path

        write(cmd_file,cmd)

        pUtil.tolog("SAGA service init")
        js=saga.job.Service("slurm+ssh://%s" % ssh_server,
                                  session=saga_session)
        pUtil.tolog("SAGA job init")
        job=js.create_job(jd)
        job.add_callback (saga.job.STATE, __state_monitor)
        pUtil.tolog("SAGA Job run")
        job.run()
        time_waisted=0
        old_state=-4
        while True:
            stn={
                saga.job.NEW:-3,
                saga.job.PENDING:-2,
                saga.job.RUNNING:-1,
                saga.job.DONE:0,
                saga.job.FAILED:1,
                saga.job.CANCELED:2,
                saga.job.SUSPENDED:3
            }.get(job.state,-4)
            if stn!=old_state:
                old_state=stn
                pUtil.tolog("Job state changed to %s"  %  job.state)
            if time_waisted>job_wait_pending and stn<-1:
                pUtil.tolog("Job is pending for too long, aborting")
                job.cancel()
                job.wait()
                continue
            if stn<0:
                time.sleep(job_wait_time)
                time_waisted+=job_wait_time/60
            else:
                break
        state=job.state
        pUtil.tolog("SAGA Job ended with state %s"%job.state)
        try:
            exit_code=job.exit_code
        except:
            exit_code=-1
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

        delete(cmd_file)

    except saga.SagaException, ex:
        # Catch all saga exceptions
        pUtil.tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        pUtil.tolog(" \n*** Backtrace:\n %s" % ex.traceback)

    pUtil.tolog("*********END**********")

def fetch_file(original,local='./',delete=False):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path,ssh_remote_home
    init_saga()

    original,rdn,rfn=__R(original)
    local,ldn,lfn=__L(local,rfn)

    pUtil.tolog("Getting remote file: %s"%original)
    pUtil.tolog("Local file path: %s"%local)
    try:
        rfile="ssh://%s%s"%(ssh_server,original)
        localpath="file://localhost%s"%local
        pUtil.tolog("Opening remote file")
        remote = saga.filesystem.File(rfile, session=saga_session)
        if delete:
            pUtil.tolog("Moving remote file")
            remote.move(localpath)
        else:
            pUtil.tolog("Copying remote file")
            remote.copy(localpath)
        pUtil.tolog("Closing file")
        remote.close()
    except saga.SagaException, ex:
        # Catch all saga exceptions
        pUtil.tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        pUtil.tolog(" \n*** Backtrace:\n %s" % ex.traceback)
    return local

def cd(dir="~"):
    global ssh_remote_path

    ssh_remote_path,x,y=__R(dir)
    return ssh_remote_path

def ls(dir="."):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path
    dir,x,y=__R(dir)
    du="ssh://%s%s"%(ssh_server,dir)
    fp = saga.filesystem.Directory(du, session=saga_session)
    list=fp.list()
    ret=[]
    for f in list:
        ret.append(str(f))
    fp.close()
    return ret

def delete(remote):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path
    init_saga()

    remote,rdn,rfn=__R(remote)

    pUtil.tolog("Deleting remote file: %s"%remote)
    rfile="ssh://%s%s"%(ssh_server,rdn)
    try:
        pUtil.tolog("Opening remote directory")
        fp = saga.filesystem.Directory(rfile, session=saga_session)
        # except saga.SagaException,ex:
        #     # Catch all saga exceptions
        #     pUtil.tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        #     # Trace back the exception. That can be helpful for debugging.
        #     pUtil.tolog(" \n*** Backtrace:\n %s" % ex.traceback)
        #     try:
        #         pUtil.tolog("Failed to open as a file, opening as a directory")
        #         fp = saga.filesystem.Directory(rfile, session=saga_session)
        #     except saga.SagaException, ex:
        #         # Catch all saga exceptions
        #         pUtil.tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        #         # Trace back the exception. That can be helpful for debugging.
        #         pUtil.tolog(" \n*** Backtrace:\n %s" % ex.traceback)
        # try:
        pUtil.tolog("Removing recursively")
        fp.remove("ssh://%s%s"%(ssh_server,remote),saga.filesystem.RECURSIVE)
        fp.close()
    except saga.SagaException, ex:
        # Catch all saga exceptions
        pUtil.tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        pUtil.tolog(" \n*** Backtrace:\n %s" % ex.traceback)

def push_file(original,remote='./'):
    global saga_session,saga_context,ssh_user,ssh_keypath,ssh_pass,ssh_remote_path
    init_saga()

    original,odn,ofn=__L(original)
    remote,rdn,rfn=__R(remote,ofn)

    pUtil.tolog("Pushing file: %s"%original)
    pUtil.tolog("Remote file path: %s"%remote)
    try:
        rfile="ssh://%s%s"%(ssh_server,remote)
        localpath="file://localhost%s"%original
        pUtil.tolog("Opening local file")
        fileWorker = saga.filesystem.File(localpath, session=saga_session)
        pUtil.tolog("Copying file")
        # file=remote.open(rfn)
        fileWorker.copy(rfile,saga.filesystem.CREATE_PARENTS)
        pUtil.tolog("Closing file")
        fileWorker.close()
    except saga.SagaException, ex:
        # Catch all saga exceptions
        pUtil.tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        pUtil.tolog(" \n*** Backtrace:\n %s" % ex.traceback)
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
