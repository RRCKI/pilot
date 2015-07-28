class NakedObject(object):
    pass


queue="bamboo-1w"

ssh=NakedObject()
ssh.user="poyda"
ssh.passwd=""
ssh.keypath="/home/apf/.ssh/sk_poyda_rsa"
ssh.server="ui2.computing.kiae.ru"
ssh.remote_home="/s/ls2/groups/g0037/panda"
ssh.remote_temp="/s/ls2/groups/g0037/tmp"
ssh.remote_bin="/s/ls2/groups/g0037/bin"

SEpath='/s/ls2/users/poyda/data'
cloudprefix='/__httpcloud__'

curl=NakedObject()
curl.cmd='curl'
curl.args='--silent --show-error --connect-timeout 100 --max-time 120 --insecure --compressed'
curl.server='https://192.168.23.43:8060/pilot'



