#!/usr/bin/env python

from time import sleep, localtime, strftime
import sys
import os, subprocess

OUTPUT_FILE = "out10"
PILOT_DIR = "/s/ls2/users/poyda/testpilot" 
PILOT_ARGS = ["./runScript"]
LAUNCH_TIMES = 50
POOL_SIZE=3

#PILOT_DIR = "."
#PILOT_ARGS = ["./pilot_emulator.py", "!!!"]

def Log(str):
	print strftime("%d %b %Y %H:%M:%S", localtime()) + "| " + str

if __name__ == "__main__":
	n = LAUNCH_TIMES
	pool = 0
	os.chdir(PILOT_DIR)
	f = open(OUTPUT_FILE, "a")
	while n > 0:
		if pool < POOL_SIZE:
			Log("%d attempts remaining."%(n))
			n = n - 1
			Log("%d of %d pilots running, starting new pilot."%(pool, POOL_SIZE))
			pid = os.fork()
			if pid == 0:
				print subprocess.call(PILOT_ARGS, stdout=f, stderr=subprocess.STDOUT)
				f.close()
				sys.exit(0)
			else:
				Log("Pilot started: child process %d."%(pid))
				pool = pool + 1
		else:
			Log("%d of %d pilots running, waiting."%(pool, POOL_SIZE))
			os.wait()
			pool = pool - 1
	while pool > 0:
		Log("%d of %d pilots running, waiting."%(pool, POOL_SIZE))
		os.wait()
		pool = pool - 1	
	Log("All attempts finished.Exiting.")
	f.close()
	sys.exit(0)
