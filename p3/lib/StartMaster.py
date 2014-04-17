#!/usr/bin/python
import os
import sys
from xml.dom import minidom


xmldoc = minidom.parse(sys.argv[1])
itemlist = xmldoc.getElementsByTagName('data')

MASTER_SERVER = itemlist[4].attributes['master_server'].value  
NUM_PARTICIPANTS = itemlist[0].attributes['num_participants'].value 
NUM_MAPS_HOST = itemlist[1].attributes['num_maps_host'].value 
NUM_REDUCES_HOST = itemlist[2].attributes['num_reduce_host'].value 
root_dir = itemlist[3].attributes['root_dir'].value 

f = os.popen("java -jar " + str(root_dir)+ "/TestMaster.jar " + MASTER_SERVER)
response = f.read()
response = response.split('\n')[0]

if(response=='0'):
	f = os.popen("ssh -n -f " + str(MASTER_SERVER) + \
             " \"sh -c \'nohup java -jar " + str(root_dir) +\
             "/Master.jar " + str(NUM_PARTICIPANTS) + \
             " " + str(NUM_MAPS_HOST) + " " + str(NUM_REDUCES_HOST) \
             + " " + root_dir + " 2>&1\'\"")	

    
