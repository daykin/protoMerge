import logging
import hashlib
import threading
import sys
import os, shutil
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
    fileHashes = set()
    result = []
    dupl = 0
    lineCount = 0
    begin = time.clock()
    for f in files:
        with open(f,"rb") as input, open(mergeFile,"w+b") as merge:
            inHash = getFileHash(input)
            if hash in fileHashes:      #see if we already looked at an exact duplicate.
                logging.info("[{:s}] {:s} is an exact duplicate of {:s}. skipping line-by-line checks and removing...".format(time.asctime(),file, hashes.get(hash)))
                lncount= len(f.readlines())
                dupl+= lncount
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
            merge.writelines(result)
            logging.info("[{:s}] completed merge for {:s} in {:f} seconds. found {:d} duplicate lines.".format(time.asctime(),f,time.clock()-begin,dupl))
    threads.remove(threading.currentThread().getName())      

def mergeOperationScheduler(subfolder1, subfolder2, maxThreads=5):          #thread to merge all PBs in a given folder
    files1 = set(file for file in os.listdir(subfolder1))
    files2 = set(file for file in os.listdir(subfolder2))
    basename = os.path.basename(subfolder1)                                 #already known this exists in both dirs 
    toMerge = files1.intersection(files2)
    singles = files1.union(files2).difference(toMerge)
    if singles:
       shutil.copy((file for file in singles),join(args.mergeFolder,basename))
       logging.info("one or more files in {:s} was not mirrored. file(s) copied to merge directory.")             
    while toMerge:
        file = toMerge.pop()
        mergedir = join(args.mergefolder,basename)
        if not os.path.exists(mergedir):
            os.makedirs(mergedir)           
        if (len(threads) < maxThreads): 
            t = threading.Thread(target=mergeOperationThread, args = ([join(subfolder1,file),join(subfolder2,file)],join(mergedir,file)))  #todo: don't reinvent wheel, use Queue
            threads.append(t.getName())
            t.start()

 
def mergeMaster(folder1, folder2):        
    subdirs1 = set(subdir for subdir in os.listdir(folder1))
    subdirs2 = set(subdir for subdir in os.listdir(folder2))
    common = subdirs1.intersection(subdirs2)
    singles = subdirs1.union(subdirs2).difference(common)
    for subdir in singles:
        logging.info("{:s} is not mirrored. copying to merged.".format(subdir))
        shutil.copy(subdir, args.mergefolder)
    for subdir in common:
        mergeOperationScheduler(join(folder1,subdir),join(folder2,subdir),args.maxthreads)

parser = argparse.ArgumentParser()
parser.add_argument("maxthreads", help = "maximum allowed parallel threads", type = int)
parser.add_argument("folder1", help = "first input directory")
parser.add_argument("folder2", help = "second input directory")
parser.add_argument("mergefolder", help = "where to place the output")
args = parser.parse_args()
                       
logging.basicConfig(filename = "merge_{:d}.log".format(int(time.time())),level = logging.INFO)
fileHashes = set()
threads = []
mergeMaster(args.folder1,args.folder2)