#!/usr/bin/env python

import time
import hashlib
import sys
from ordered_set import OrderedSet

#uncomment for logging
#sys.stdout = open("log.txt",'w')

def removeDuplicates(lines): 
   seen = {}
   result = []
   dupl = 0
   for item in lines:
       if item in seen: 
           dupl+=1
           continue
       seen[item] = 1
       result.append(item)
   #seen = {}
   print "removed {:d} duplicates.".format(dupl,cnt)
   return result


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
    #lines = []
    merged.close()
    print "done. total Time: {:f}.".format(time.clock()-timestart)

files = ["C2014~1.PB","C2014~2.PB"]
readAndMerge(files)
