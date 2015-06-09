#!/usr/bin/env python

from time import sleep, localtime, strftime
import sys
import os, subprocess

OUTPUT_FILE = "out10"
PILOT_DIR = "/s/ls2/users/poyda/testpilot" 
PILOT_ARGS = ["./runScript"]
LAUNCH_TIMES = 10

#PILOT_DIR = "."
#PILOT_ARGS = ["./pilot_emulator.py", "!!!"]

def Log(str):
	print strftime("%d %b %Y %H:%M:%S", localtime()) + "| " + str

if __name__ == "__main__":
	n = LAUNCH_TIMES
	os.chdir(PILOT_DIR)
	f = open(OUTPUT_FILE, "a")
	while n > 0:
		Log("%d attempts remaining."%(n))
		n = n - 1
		print subprocess.call(PILOT_ARGS, stdout=f, stderr=subprocess.STDOUT)
	Log("All attempts finished.Exiting.")
	f.close()
	sys.exit(0)
