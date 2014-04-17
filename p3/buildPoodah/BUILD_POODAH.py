#!/usr/bin/python

import os
import sys
curr_dir = os.getcwd()

build_files = [curr_dir + "/ProcMan.xml", curr_dir + "/MasterBuild.xml", curr_dir + "/TestMaster.xml", curr_dir + "/WorkerBuild.xml"]

os.chdir("..")
lib_dir = os.getcwd() + "/lib"
os.chdir("lib")
os.popen("rm *.jar 2>&1 > /dev/null")

for file in build_files:
	f = os.popen("ant -f " + file)
	f.read()

sample_dir = [curr_dir + "/WordCountBuild.xml", curr_dir + "/WordLengthBuild.xml"]

for file in sample_dir:
	f = os.popen("ant -f " + file);
	f.read()

print "Poodah Lib Directory:"
print lib_dir

