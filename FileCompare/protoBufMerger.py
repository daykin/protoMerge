import logging
import hashlib
import threading
import sys
import os
from os.path import join
import time
import EPICSEvent_pb2 as pb
import argparse

def getFileHash(file):                  #return the hash of an entire file.
    m = hashlib.md5()
    for line in file:
        m.update(line.strip())
    file.seek(0)    
    return m.hexdigest()

def mergeOperationThread(files, mergeFile):    
    seen = {}
    result = []
    dupl = 0
    lineCount = 0
    for f in files:
        try:
            with open(f,"rb") as input:
                inHash = getFileHash(input)
                if hash in hashes:      #see if we already looked at an exact duplicate.
                    logging.info("[{:s}] {:s} is an exact duplicate of {:s}. skipping line-by-line checks and removing...".format(time.asctime(),file, hashes.get(hash)))
                    lncount= len(f.readlines())
                    dupl+= duplncount
                    lineCount += duplncount
                    f.close()
                    os.remove(file)
                    continue
                fileHashes.add(inHash)
                for line in input:      #check line-by-line. add uniques to a dict for speedy lookup
                    lineCount += 1
                    if line in seen: 
                        dupl+=1
                        continue
                    seen[line] = 1      #add entry to dict
                    result.append(line) #add the unique to a list to retain order
        except:
             logging.warn("no corresponding file in opposite folder.")
             
             continue 
    threads.remove(threading.currentThread().getName())      

def mergeOperationScheduler(subfolder1, subfolder2, maxThreads):          #thread to merge all PBs in a given folder
    files1 = [file for file in os.listdir(subfolder1)]
    files2 = [file for file in os.listdir(subfolder2)]
    files1.extend(files2)
    files = set(files1)
    while files:
        if (len(threads) < maxThreads): 
            t = threading.Thread(target=mergeOperationThread, args = ([join(subfolder1,file),join(subfolder2,file)]))  #todo: don't reinvent wheel, use Queue
            threads.append(t.getName())
            t.start()

 
def mergeMaster(folder1, folder2, maxThreads=5):       
    subdirs = [subdir for subdir in os.listdir(folder1)]
    for subdir in subdirs:
        mergeOperationScheduler(join(folder1,subdir),join(folder2,subdir),)


parser = argparse.ArgumentParser()
parser.add_argument("maxThreads", help = "maximum allowed parallel threads", type = int)
parser.add_argument("folder1", help = "first merge directory")
parser.add_argument("folder2", help = "second merge directory")
parser.add_argument("mergeFolder", help = "where to place the output")
args = parser.parse_args()
                       
logging.basicConfig(filename = "merge_{:d}.log".format(int(time.time())),level = logging.INFO)
fileHashes = set()
threads = []
mergeScheduler("app1","app2", "merge",5)