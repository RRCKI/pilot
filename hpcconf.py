class NakedObject(object):
        pass

queue = 'SK_batch_queue'
ssh = NakedObject()
ssh.user = 'bbpvizuser'
ssh.passwd = ''
ssh.keypath = ''
ssh.server = 'bbplxviz1.epfl.ch'
ssh.port = ''
ssh.remote_home = '/s/ls2/groups/g0037/panda'
ssh.remote_temp = '/s/ls2/groups/g0037/tmp'
ssh.remote_bin = '/s/ls2/groups/g0037/bin'

MYDB={
    'host':'127.0.0.1',
    'port':3306,
    'db':'pilot2',
    'user':'pilot',
    'passwd':'pandapilot'
}

#NEXT need for SE only (with Portal) 

SEpath='/home/bbpvizuser/PandaTest/home'
cloudprefix='/__httpcloud__'
stageout_path='/s/ls2/users/poyda/data/system/{scope}/.sys/{guid}/{file}'
#stagein_path='/s/ls2/users/poyda/data/system/{scope}/.sys/{guid}/{file}'
stagein_path='/s/ls2/users/poyda/data/system/{scope}/{guid}/{file}'

curl=NakedObject()
curl.cmd='curl'
curl.args='--silent --show-error --connect-timeout 100 --max-time 120 --insecure --compressed'
curl.server='https://192.168.23.43:8060/pilot'
