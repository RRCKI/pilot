
import saga
import os, sys, commands, time

waittime=1


def create_remote_SLURM_session():
    ctx=saga.Context("ssh")
    ctx.user_id   = "poyda"
    ctx.user_cert = "/home/apf/.ssh/sk_poyda_rsa" # private key derived from cert
    ctx.user_pass = ""

    session = saga.Session()
    _session=session
    session.add_context(ctx)

    js = saga.job.Service("slurm+ssh://ui2.computing.kiae.ru",
              session=session)
    return session

def putFilesRemote_SLURM(local_dir_path, remote_machine, remote_file_path):
        try:
            #remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path+"/../../")
            #list_tokens = remote_file_path.split('/')
            #f1=list_tokens[:-1]
            #remote_directory.make_dir(f1, saga.filesystem.CREATE)
            #remote_directory.close()
            #remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + f1)
            #remote_directory.make_dir(remote_file_pat, saga.filesystem.CREATE)
            #remote_directory.close()
            #remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_pat)

            #remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path)
            #remote_directory = saga.namespace.Directory("sftp://" + remote_machine + remote_file_path+"/A1/A2/../../")
            #remote_directory.open_dir('/dat', saga.namespace.CREATE)
            #remote_directory.make_dir(remote_file_path+"/A1/A2/", saga.filesystem.CREATE)
            #remote_directory.close()
            #remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path + "/A1/A2/")
            remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path+"/../../")
            f1 = remote_file_path.rsplit('/', 1)[0]
            print f1
            remote_directory.make_dir(f1, saga.filesystem.CREATE)
            remote_directory.close()
            remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + f1)
            remote_directory.make_dir(remote_file_path, saga.filesystem.CREATE)
            remote_directory.close()
            remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path)
            #print("OK")

        except saga.SagaException, ex:
            print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
            print(" \n*** Backtrace:\n %s" % ex.traceback)
            sys.stdout.flush()
            return

        try:
            local_path = "file://localhost"+local_dir_path
            local_directory = saga.filesystem.Directory(local_path)
        except saga.SagaException, ex:
            print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
            print(" \n*** Backtrace:\n %s" % ex.traceback)
            remote_directoryclose();
            return

        try:
            #files = remote_directory.list()
            files = local_directory.list()
            for f in files:
                if local_directory.is_file(f):
                    local_directory.copy(f, "sftp://" + remote_machine + remote_file_path)
                else:
                    name = "%s" % f
                    remote_directory.make_dir(name+"/")
                    putFilesRemote_SLURM(local_dir_path+"/"+name, remote_machine, remote_file_path+"/"+name)

            remote_directory.close()
            local_directory.close()
        except saga.SagaException, ex:
            print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
            print(" \n*** Backtrace:\n %s" % ex.traceback)

        remote_directory.close();
        local_directory.close();
        return


def getFilesRemote_SLURM(local_dir_path, remote_machine, remote_file_path, session):
    try:
	#remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path)
	#remote_directory = saga.namespace.Directory("sftp://" + remote_machine + remote_file_path+"/A1/A2/../../")
	#remote_directory.open_dir('/dat', saga.namespace.CREATE)
	#remote_directory.make_dir(remote_file_path+"/A1/A2/", saga.filesystem.CREATE)
	#remote_directory.close()
	#remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path + "/A1/A2/")
	remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path+"/../../")
	f1 = remote_file_path.rsplit('/', 1)[0]
	print f1
	remote_directory.make_dir(f1, saga.filesystem.CREATE)
	remote_directory.close()
	remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + f1)
	remote_directory.make_dir(remote_file_path, saga.filesystem.CREATE)
	remote_directory.close()
	remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path)
	print("OK")
	                                                                                                
    except saga.SagaException, ex:
	print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        print(" \n*** Backtrace:\n %s" % ex.traceback)
	return
	
    try:
	local_path = "file://localhost"+local_dir_path
	local_directory = saga.filesystem.Directory(local_path)
    except saga.SagaException, ex:
	print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        print(" \n*** Backtrace:\n %s" % ex.traceback)
        remote_directoryclose();
	return
	
    try:
	files = remote_directory.list()
	for f in files:
	    if remote_directory.is_file(f):
		remote_directory.copy(f, local_path)
	    else:
		name = "%s" % f
	        local_directory.make_dir(name+"/")
		getFilesRemote_SLURM(local_dir_path+"/"+name, remote_machine, remote_file_path+"/"+name)

	remote_directory.close()
	local_directory.close()
    except saga.SagaException, ex:
	print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        print(" \n*** Backtrace:\n %s" % ex.traceback)

    remote_directory.close();
    local_directory.close();
    return
    
def putCertificate():
    try:
	#tolog("certificate transfer....")
        outfilesource = 'sftp://localhost/tmp/x509up_u500'
        dirtarget = 'ssh://ui2.computing.kiae.ru/s/ls2/users/poyda/ddd'
        #print("copy %s to %s" % (outfilesource, dirtarget) )
        #out = saga.filesystem.File(outfilesource, session=self.__saga_session)^M
        #out.copy(outfiletarget)^M
        #outfilesource = 'sftp://ui2.computing.kiae.ru/'+job.workdir+'/'+job.stderr^M
        #out = saga.filesystem.File(outfilesource, session=self.__saga_session)^M
        #out.copy(outfiletarget)^M
        #out.close()^M
        #outfilesource = 'ssh://ui2.computing.kiae.ru/'+job.workdir+'/results'
        #outfiletarget = 'file:///home/apf/'
        #print("copy %s to %s" % (outfilesource, outfiletarget) )
        #out = saga.filesystem.File(outfilesource, session=self.__saga_session)^M
        #out.copy(outfiletarget)^M
        #out.close()
        print("PP")
        fl = saga.filesystem.File('file://localhost/tmp/x509up_u500')
        print("OK")
        fl.copy(dirtarget+"X509up_u500")
    except saga.SagaException, ex:
	# Catch all saga exceptions^M
        print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.^M
        print(" \n*** Backtrace:\n %s" % ex.traceback)^M
    print("done");


def del_remote(remote_machine, remote_file_path):
        try:
            #remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path+"/../../")
            #list_tokens = remote_file_path.split('/')
            #f1=list_tokens[:-1]
            #remote_directory.make_dir(f1, saga.filesystem.CREATE)
            #remote_directory.close()
            #remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + f1)
            #remote_directory.make_dir(remote_file_pat, saga.filesystem.CREATE)
            #remote_directory.close()
            #remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_pat)

            #remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path)
            #remote_directory = saga.namespace.Directory("sftp://" + remote_machine + remote_file_path+"/A1/A2/../../")
            #remote_directory.open_dir('/dat', saga.namespace.CREATE)
            #remote_directory.make_dir(remote_file_path+"/A1/A2/", saga.filesystem.CREATE)
            #remote_directory.close()
            #remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path + "/A1/A2/")
            remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path)
            remote_directory.remove(remote_file_path, saga.filesystem.RECURSIVE);
            return
            f1 = remote_file_path.rsplit('/', 1)[0]
            print f1
            remote_directory.make_dir(f1, saga.filesystem.CREATE)
            remote_directory.close()
            remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + f1)
            remote_directory.make_dir(remote_file_path, saga.filesystem.CREATE)
            remote_directory.close()
            remote_directory = saga.filesystem.Directory("sftp://" + remote_machine + remote_file_path)
            #print("OK")

        except saga.SagaException, ex:
            print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
            print(" \n*** Backtrace:\n %s" % ex.traceback)
            sys.stdout.flush()
            return

        try:
            local_path = "file://localhost"+local_dir_path
            local_directory = saga.filesystem.Directory(local_path)
        except saga.SagaException, ex:
            print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
            print(" \n*** Backtrace:\n %s" % ex.traceback)
            remote_directoryclose();
            return

        try:
            #files = remote_directory.list()
            files = local_directory.list()
            for f in files:
                if local_directory.is_file(f):
                    local_directory.copy(f, "sftp://" + remote_machine + remote_file_path)
                else:
                    name = "%s" % f
                    remote_directory.make_dir(name+"/")
                    putFilesRemote_SLURM(local_dir_path+"/"+name, remote_machine, remote_file_path+"/"+name)

            remote_directory.close()
            local_directory.close()
        except saga.SagaException, ex:
            print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
            print(" \n*** Backtrace:\n %s" % ex.traceback)

        remote_directory.close();
        local_directory.close();
        return

create_remote_SLURM_session()
del_remote('ui2.computing.kiae.ru', '/s/ls2/users/poyda/testpilot/A1/')
sys.exit()
putCertificate()



try:
    #http://saga-python.readthedocs.org/en/latest/adaptors/saga.adaptor.slurm_job.html
    ctx=saga.Context("ssh")
    ctx.user_id   = "poyda"
    ctx.user_cert = "/home/apf/.ssh/sk_poyda_rsa" # private key derived from cert
    ctx.user_pass = ""

    session = saga.Session()
    session.add_context(ctx)

    js = saga.job.Service("slurm+ssh://ui2.computing.kiae.ru",
              session=session)

    #rm = saga.resource.Manager("slurm+ssh://ui2.computing.kiae.ru", session=session)

    #print("Conpute res = : %s" % (rm.list(COMPUTE)))

    #jd = saga.job.Description()
    # jd.project = 'CSC108' # should be taken from resourse description (pandaqueue)
    #jd.wall_time_limit = 60*24*6 
    #jd.executable      = "/s/ls2/home/users/complynx/testsaga-s2.sh"
    #jd.executable = "#!/bin/sh\n\nhostname\n"
    
    
    
    #jd.total_cpu_count = 1
    #jd.output = "examplejob4.out"
    #jd.error = "examplejob4.err"
    #jd.queue = "bamboo-1w"   # should be taken from resourse description (pandaqueue)
    #jd.working_directory = "/s/ls2/home/users/poyda/sagatest/d/"
    
    #fork_job = js.create_job(jd)
    #fork_job.add_callback(saga.STATE, self.jobStateChangeNotification)
    
    #print("\n(PBS) Command: %s\n"  % to_script)
    #fork_job.run()
    #print "Local Job ID: %s" % fork_job.id
    
    #for i in range(waittime * 1):
    # time.sleep(10)
    # if fork_job.state != saga.job.PENDING:
    #     break
    #if fork_job.state == saga.job.PENDING:
    # repeat_num = repeat_num + 1
    # print "Wait time (%s s.) exceed" % (waittime)
    # fork_job.cancel()
    # fork_job.wait()
        
     #print("Wait time (%s s.) exceed, job cancelled" % waittime)
    
    
    #fork_job.wait()
    #print("Job State              : %s" % (fork_job.state))
    #print("Exitcode               : %s" % (fork_job.exit_code))
    #print("Create time            : %s" % (fork_job.created))
    #print("Start time             : %s" % (fork_job.started))
    #print("End time               : %s" % (fork_job.finished))
    #print("Walltime limit         : %s (min)" % (jd.wall_time_limit))
    
    
    #print("Allocated cores        : %s" % (cpu_number))
    #cons_time = datetime.strptime(fork_job.finished, '%c') - datetime.strptime(fork_job.started, '%c')
    #cons_time_sec =  (cons_time.microseconds + (cons_time.seconds + cons_time.days * 24 * 3600) * 10**6) / 10**6
    #print("Execution time         : %s (sec. %s)" % (str(cons_time), cons_time_sec))
    #job.timeExe = int(fork_job.finished - fork_job.started)
    
    #outfilesource = 'sftp://ui2.computing.kiae.ru/s/ls2/home/users/poyda/sagatest/d/examplejob4.out'
    outfilesource = 'sftp://ui2.computing.kiae.ru/s/ls2/home/users/poyda/sagatest/d/'
    #outfilesource = 'sftp://gw68.quarry.iu.teragrid.org/users/oweidner/mysagajob.stdout'
    outfiletarget = 'file://localhost/tmp/d/'
    
    #jd = saga.job.Description()
    # jd.project = 'CSC108' # should be taken from resourse description (pandaqueue)
    
    #jd.wall_time_limit = 60*24*6 
    #jd.executable      = "/s/ls2/home/users/complynx/testsaga-s2.sh"
    #jd.executable = "#!/bin/sh\n\nls > __ls.res\n"
    
    
    
    #jd.total_cpu_count = 1
    #jd.output = "examplejob4.out"
    #jd.error = "examplejob4.err"
    #jd.queue = "bamboo-1w"   # should be taken from resourse description (pandaqueue)
    #jd.working_directory = "/s/ls2/home/users/poyda/sagatest/d/"
    
    #fork_job = js.create_job(jd)
    #fork_job.add_callback(saga.STATE, self.jobStateChangeNotification)
    
    #print("\n(PBS) Command: %s\n"  % to_script)
    #fork_job.run()
    #print "Local Job ID: %s" % fork_job.id
    
    #for i in range(waittime * 1):
    # time.sleep(10)
    # if fork_job.state != saga.job.PENDING:
    #     break
    #if fork_job.state == saga.job.PENDING:
    # repeat_num = repeat_num + 1
    # print "Wait time (%s s.) exceed" % (waittime)
    # fork_job.cancel()
    # fork_job.wait()
        
     #print("Wait time (%s s.) exceed, job cancelled" % waittime)
    
    
    #fork_job.wait()
    #print("Job State              : %s" % (fork_job.state))
    #print("Exitcode               : %s" % (fork_job.exit_code))
    #print("Create time            : %s" % (fork_job.created))
    #print("Start time             : %s" % (fork_job.started))
    #print("End time               : %s" % (fork_job.finished))
    #print("Walltime limit         : %s (min)" % (jd.wall_time_limit))
    
    #out = saga.filesystem.File(outfilesource, session=session)
    #out = saga.filesystem.Directory(outfilesource, session=session)
    #dird = saga.filesystem.Directory("sftp://ui2.computing.kiae.ru/s/ls2/home/users/poyda/sagatest/d/",
    #	saga.filesystem.READ,session)
    dird = saga.filesystem.Directory("sftp://ui2.computing.kiae.ru/s/ls2/home/users/poyda/sagatest/d/")
    #dird.make_dir("data/")
    #data = dird.open_dir ("data2/", saga.filesystem.Create)
    files = dird.list()
    for f in files:
	print f
    for f in files:
	dird.copy(f, outfiletarget)
    #dir.copy ("./data.bin", "sftp://localhost/tmp/data/")
    #out.copy(outfiletarget)
    
    #print "Staged out %s to %s (size: %s bytes)" % (outfilesource, outfiletarget, out.get_size())
    
    
    #out.close()
    dird.close()
    ####################################################
except saga.SagaException, ex:
    # Catch all saga exceptions
    print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
    # Trace back the exception. That can be helpful for debugging.
    print(" \n*** Backtrace:\n %s" % ex.traceback)
    #if out:
    #	out.close()
    if dird:
	dird.close()

