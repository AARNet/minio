#!/usr/bin/python -u
# encoding=utf8

# Writes chunks into EOS using xrootd because we do not want to re-calculate checksums on each upload.

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
checksum = int(sys.argv[4])
uid = int(sys.argv[5])
gid = int(sys.argv[6])

extra = "?eos.ruid="+str(uid)+"&eos.rgid="+str(gid)

if (checksum == 0):
	extra = extra + "&eos.checksum=ignore"

bookingsize = "&eos.bookingsize=" + str(size)

with client.File() as f:
	status,null = f.open(filename + extra + bookingsize,flags=OpenFlags.NEW)

	opencount = 0
	while status.ok == False:
		f = client.File()
		status,null = f.open(filename + extra,flags=OpenFlags.UPDATE)
		if (status.ok == False):
			time.sleep(0.1)
			f = client.File()
			status,null = f.open(filename + extra,flags=OpenFlags.UPDATE)
			if (status.ok == False):
				if (opencount<=100):
					opencount += 1
					time.sleep(0.1)
					f = client.File()
					status,null = f.open(filename + extra + bookingsize,flags=OpenFlags.NEW)
				else:
					log_print("ERROR: open()")
					sys.exit(status)

	if (opencount > 0):
		log_print("WARNING: Failed to open file on CloudStor " + str(opencount) + " times")
	
	if (offset >= 0):
		status,null = f.write(sys.stdin.read(),offset=offset)
		if (status.ok == False):
			log_print("ERROR: write()")
			sys.exit(status)

	if (checksum != 0):
		status,null = f.truncate(size)
		if (status.ok == False):
			log_print("ERROR: truncate()")
			sys.exit(status)

