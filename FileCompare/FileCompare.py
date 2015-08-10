import hashlib
import shutil
import os
from functools import partial
import EPICSEvent_pb2 as pb
import time
import pandas as pd


escapeTo = [str(unichr(27)), str(unichr(10)), str(unichr(13))] #esc, nl, cr

def mergeDuplicateFiles(filename1,filename2):
    if(fileDigest(filename1)==fileDigest(filename2)):
        mergedFile = open('merged.PB','wb')
        shutil.copy(filename1,filename2)
        os.remove(filename1)
        os.remove(filename2)
        mergedFile.close()

def fileDigest(filename):
    f1 = open(filename,'rb')
    hash = hashlib.sha256()
    for buf in iter(partial(f1.read, 128),b''):
        hash.update(buf)
    digest = hash.hexdigest()
    f1.close()
    return digest


def readfile(filename):
    numlines=0
    lines = []
    with open(filename, 'rb') as f:
        digest = filedigest(f)
        lines = f.readlines()
    if(digest not in hashes):
        hashes.append(digest)
        numlines += len(lines)
        timestart = time.clock()
        expectheader = true
        for line in lines:
            line = line.strip()
            if not line:
                expectheader = true
                continue

            foundescape = false
            unescapedline = ""
            for char in line:
                if ord(char) == 27: #escape char
                    #print "found escape"
                    foundescape = true
                else:
                    if foundescape:
                        foundescape = false
                        unescapedline = unescapedline + escapeto[ord(char)-1]
                        # print "adding escape for ",  ord(char) 
                    else:
                        # print "adding regular char ", ord(char) 
                        unescapedline = unescapedline + char


            if expectheader:
                i = pb.payloadinfo()
                i.parsefromstring(unescapedline)
                print ("processed header for ", i.pvname, " for year ", i.year)
                expectheader = false
            else:
                d = pb.scalardouble()
                try:
                    d.parsefromstring(unescapedline)
                except:
                    pass
                #print ("processed value with secondsintoyear",d.secondsintoyear, "and nanos", d.nano, "and val", d.val, "and severity", d.severity, " and status", d.status)
                #print("{:d}.{:d}").format(d.secondsintoyear,d.nano)
        print("processed {:d} records in {:f} seconds").format(numlines,(time.clock())-timestart)
    else:
         pass
    f.close()

hashes = []
mergeDuplicateFiles('test1.txt','test2.txt')

#print(compareFiles("C2015_~1.PB","C2015_~2.PB"))

