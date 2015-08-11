import logging
import hashlib
import threading
import glob as gl
import sys
import os
from os.path import join
import time
from collections import OrderedDict

def getFileHash(file): #return the hash of an entire file.
    m = hashlib.md5()
    for line in file:
        m.update(line.strip())
    file.seek(0)    
    return m.hexdigest()

def mergeWorkerThread(folder): #thread to merge all PBs in a given folder
    hashes = {} 
    seen = {}
    result = []
    dupl = 0
    lineCount = 0
    begin = time.clock()
    logging.info("[{:s}] Starting merge for {:s}.".format(time.asctime(), folder)) 
    with open(join(folder,"{:s}_merged.PB".format(folder)),"wb") as merged:  #open an output file
        for file in gl.glob(join(folder,"*.pb")):
            with open(file, "rb") as f:
                hash = getFileHash(f) 
                if hash in hashes: #see if we already looked at an exact duplicate.
                    logging.info("{:s} is an exact duplicate of {:s}. skipping line-by-line checks and removing...".format(file, hashes.get(hash)))
                    lncount= len(f.readlines())
                    dupl+= duplncount
                    lineCount += duplncount
                    f.close()
                    os.remove(file)
                    continue
                hashes[hash] = file  #if not, record the unique
                for line in f:       #check line-by-line. add uniques to a dict
                    lineCount += 1
                    if line in seen: 
                        dupl+=1
                        continue
                    seen[line] = 1
                    result.append(line)                    
        merged.writelines(result)
    logging.info("[{:s}] Merged files for {:s}. Operation took {:f} seconds. {:d} records processed with {:d} duplicate(s).".format(time.asctime(),folder,time.clock()-begin,lineCount,dupl))
    threads.remove(threading.currentThread().getName())
        
def mergeScheduler(maxThreads=5):       
    for folder in gl.glob("20*"):
        folders.append(folder)
    while folders:        
        if (len(threads) < maxThreads):
            t = threading.Thread(target=mergeWorkerThread,args=(folders.pop(),))
            threads.append(t.getName())
            t.start()
                       
logging.basicConfig(filename = "merge_{:d}.log".format(int(time.time())),level = logging.INFO)

threads = []
folders = []
mergeScheduler()


