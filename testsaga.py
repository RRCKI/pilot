
import saga
import os, sys, commands, time

waittime=1

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

    rm = saga.resource.Manager("slurm+ssh://ui2.computing.kiae.ru", session=session)

    print("Conpute res = : %s" % (rm.list(COMPUTE)))

    # jd = saga.job.Description()
    # # jd.project = 'CSC108' # should be taken from resourse description (pandaqueue)
    # jd.wall_time_limit = 60*24*6 
    # jd.executable      = "/s/ls2/home/users/complynx/testsaga-s2.sh"
    # jd.total_cpu_count = 1
    # jd.output = "examplejob.out"
    # jd.error = "examplejob.err"
    # jd.queue = "bamboo-1w"   # should be taken from resourse description (pandaqueue)
    # jd.working_directory = "/s/ls2/home/users/poyda/sagatest/d/"
    
    # fork_job = js.create_job(jd)
    # #fork_job.add_callback(saga.STATE, self.jobStateChangeNotification)
    
    # #print("\n(PBS) Command: %s\n"  % to_script)
    # fork_job.run()
    # print "Local Job ID: %s" % fork_job.id
    
    # for i in range(waittime * 1):
    #     time.sleep(60)
    #     if fork_job.state != saga.job.PENDING:
    #         break
    # if fork_job.state == saga.job.PENDING:
    #     repeat_num = repeat_num + 1
    #     print "Wait time (%s s.) exceed" % (waittime)
    #     fork_job.cancel()
    #     fork_job.wait()
        
    #     #print("Wait time (%s s.) exceed, job cancelled" % waittime)
    
    
    # fork_job.wait()
    # print("Job State              : %s" % (fork_job.state))
    # print("Exitcode               : %s" % (fork_job.exit_code))
    # print("Create time            : %s" % (fork_job.created))
    # print("Start time             : %s" % (fork_job.started))
    # print("End time               : %s" % (fork_job.finished))
    # print("Walltime limit         : %s (min)" % (jd.wall_time_limit))
    # print("Allocated cores        : %s" % (cpu_number))
    # cons_time = datetime.strptime(fork_job.finished, '%c') - datetime.strptime(fork_job.started, '%c')
    # cons_time_sec =  (cons_time.microseconds + (cons_time.seconds + cons_time.days * 24 * 3600) * 10**6) / 10**6
    # print("Execution time         : %s (sec. %s)" % (str(cons_time), cons_time_sec))
    # #job.timeExe = int(fork_job.finished - fork_job.started)
    
    
    ####################################################
except saga.SagaException, ex:
    # Catch all saga exceptions
    print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
    # Trace back the exception. That can be helpful for debugging.
    print(" \n*** Backtrace:\n %s" % ex.traceback)

