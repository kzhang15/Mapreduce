#!/usr/bin/python

import os
import sys
from xml.dom import minidom


xmldoc = minidom.parse(sys.argv[1])
itemlist = xmldoc.getElementsByTagName('data')

userid = itemlist[5].attributes['userid'].value
print userid

os.popen("ssh -n -f " + str(sys.argv[2]) + " -o StrictHostKeyChecking=no \"sh -c \'nohup killall -u " + str(userid) + " 2>&1 &\'\"")
