#!/usr/bin/python

import os
import sys

if(len(sys.argv) < 4):
    print "args: <Master_ip> <hostname_worker>"

MASTER_IP = sys.argv[1]
WORKER_HOST = sys.argv[2]
root_dir = sys.argv[3]
print root_dir

f = os.popen("ssh -n -f " + str(WORKER_HOST) + \
             " -o StrictHostKeyChecking=no \"sh -c \'nohup java -jar " \
							+ str(root_dir) + "/Worker.jar " + str(MASTER_IP) + " 2>&1 &\'\"")
#response = f.read()
