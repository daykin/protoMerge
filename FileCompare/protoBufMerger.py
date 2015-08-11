import logging
import hashlib
import threading
import glob as gl
import sys
import os
from os.path import join
import time

def getFileHash(file):
    m = hashlib.md5()
    for line in file:
        m.update(line.strip())
    file.seek(0)    
    return m.hexdigest()

def mergeWorkerThread(folder):
    hashes = {}
    seenLines = {}
    result = []
    dupl = 0
    lineCount = 0
    begin = time.clock()
    logging.info("[{:s}]Starting merge for {:s}.".format(time.asctime(), folder)) 
    with open(join(folder,"{:s}_merged.PB".format(folder)),"wb") as merged:
        for file in gl.glob(join(folder,"*.pb")):
            with open(file, "rb") as f:
                hash = getFileHash(f)
                if hash in hashes:
                    logging.info("{:s} is an exact duplicate of {:s}. skipping line-by-line checks and removing...".format(file, hashes.get(hash)))
                    dupl += len(f.readlines())
                    f.close()
                    os.remove(file)
                    continue
                hashes[hash] = file
                for line in f:
                    lineCount += 1
                    if line in seen: 
                        dupl+=1
                        continue
                    seen[line] = 1
                    result.append(line)                    
        merged.writelines(result)
    logging.info("[{:s}] merged files for {:s}.operation took {:f} seconds. {:d} records with {:d} duplicate(s).".format(time.asctime(),folder,time.clock()-begin,lineCount,dupl))
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
sys.exit()

