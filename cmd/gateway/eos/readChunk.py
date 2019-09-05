#!/usr/bin/python -u
# encoding=utf8

# Reads chunks from EOS using xrootd and outputs to stdout

import sys
import json
import time
import syslog
from XRootD import client
from XRootD.client.flags import *
from XRootD.client.responses import *

def log_print(s):
	print >> sys.stderr, s

reload(sys)
sys.setdefaultencoding('utf8')

filename = sys.argv[1]
offset = int(sys.argv[2])
size = int(sys.argv[3])
uid = int(sys.argv[4])
gid = int(sys.argv[5])

extra = "?eos.ruid="+str(uid)+"&eos.rgid="+str(gid)

with client.File() as f:
	status,null = f.open(filename + extra,flags=OpenFlags.READ)

	opencount = 0
	while status.ok == False:
		if (opencount<=100):
			opencount += 1
			time.sleep(0.1)
			f = client.File()
			status,null = f.open(filename + extra,flags=OpenFlags.READ)
		else:
			log_print("ERROR: open()")
			sys.exit(status)

	if (opencount > 0):
		log_print("WARNING: Failed to open file on CloudStor " + str(opencount) + " times")
	
	status, data = f.read(offset=offset,size=size)
        if (status.ok):
                print data
        else:
		log_print("ERROR: read() "+status.message)
		sys.exit(status)
