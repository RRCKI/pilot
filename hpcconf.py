class NakedObject(object):
    pass


queue="pledged"

ssh=NakedObject()
ssh.user="apfc"
ssh.passwd=""
ssh.keypath="/home/apf/.ssh/apfc_rsa"
ssh.server="192.168.23.71"
ssh.remote_home="/home/apfc"
ssh.remote_temp="/tmp"

SEpath='/data/pilots'
cloudprefix='/__httpcloud__'

curl=NakedObject()
curl.cmd='curl'
curl.args='--silent --show-error --connect-timeout 100 --max-time 120 --insecure --compressed'
curl.server='https://192.168.23.43:8060/api'



