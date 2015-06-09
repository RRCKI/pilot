#!/usr/bin/env python

from time import sleep, localtime, strftime
from sys import exit, argv

def Log(str):
	print strftime("%d %b %Y %H:%M:%S", localtime()) + "| " + str

if __name__ == "__main__":
	t = 6
	Log("Starting pilot emulator.")
	for i in range(1, len(argv)):
		Log("Argument %d = %s."%(i, argv[i]))
	Log("Now sleeping for %d seconds."%(t))
	sleep(t)
	Log("Done sleeping. Exiting.")
	exit(0)
