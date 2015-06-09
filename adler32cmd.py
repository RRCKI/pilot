""" calculate the checksum for a file with the zlib.adler32 algorithm """
# note: a failed file open will return '1'

import zlib,sys,os
# default adler32 starting value
sum1 = 1L

if len(sys.argv) < 2:
    sys.exit('Usage: %s filename [output_prefix]' % sys.argv[0])

if not os.path.exists(sys.argv[1]):
    sys.exit('ERROR: File %s was not found!' % sys.argv[1])
filename=sys.argv[1]

prefix=''
if(len(sys.argv))>2:
    prefix=sys.argv[2]

f = open(filename, 'rb')
for line in f:
    sum1 = zlib.adler32(line, sum1)

f.close()

# correct for bug 32 bit zlib
if sum1 < 0:
    sum1 = sum1 + 2**32

# convert to hex
sum2 = "%08x" % sum1

print(prefix+str(sum2))
