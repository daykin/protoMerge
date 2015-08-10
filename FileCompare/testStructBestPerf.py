#!/usr/bin/env python

import time
import hashlib
import sys

#uncomment for logging
#sys.stdout = open("log.txt",'w')




def readAndMerge(files):    
    timestart=time.clock()
    numRecords = 0
    lines = []
    merged = open("C2014.pb","wb")
    for i in range(len(files)):        
        with open(files[i],"rb") as f:                        
            lines.extend(f.readlines())                  
            f.close()
    numRecords = len(lines)
    lines = removeDuplicates(lines)
    merged.writelines(lines)
    merged.close()
    print "done. total Time: {:f}.".format(time.clock()-timestart)

files = ["C2015_~1.PB","C2015_~2.PB"]
readAndMerge(files)
