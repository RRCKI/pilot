
import sys,os,time

if len(sys.argv) < 2:
    sys.exit('Usage: %s filename [output_prefix]' % sys.argv[0])

if not os.path.exists(sys.argv[1]):
    sys.exit('ERROR: File %s was not found!' % sys.argv[1])
filename=sys.argv[1]

t=time.strftime("%Y-%m-%d %I:%M:%S",time.localtime(os.path.getmtime(filename)))
fsize = str(os.path.getsize(filename))

print('modtime:%s\nfsize:%s'%(t,fsize))

