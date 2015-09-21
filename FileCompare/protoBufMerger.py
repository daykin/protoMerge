import logging
import hashlib
import threading
import sys
import os, shutil, errno
import glob
from os.path import join
import time
import argparse

def getFileHash(file):                  	#return the hash of an entire file.
    m = hashlib.md5()
    for line in file:
        m.update(line.strip())
    file.seek(0)    
    return m.hexdigest()

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:                  #EAFP
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def getRelativeParents(f):
    backIdx = f.rfind("\\")
    fwdIdx = f.rfind ("/")
    if not(backIdx == -1):
        return f[0:backIdx+1]
    elif not(fwdIdx == -1):
        return f[0:fwdIdx+1]
    elif not(backIdx == -1 or backIdx == -1):
        raise NameError("found malformed filename {:s}. contains forward and back slashes.".format(f))
    else:
        return f

def enumerate_files(directory):
    paths = []

    for root, directories, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root.replace(directory,''), filename)
            paths.append(filepath)
    return paths

def mergeOperationThread(files, mergeFile):    
    seen = {}
    result = []
    dupl = 0
    lineCount = 0
    begin = time.clock()
    for f in files:
        with open(f,"rb") as input, open(mergeFile,args.mergemode+"+b") as merge:
            inHash = getFileHash(input)
            if inHash in fileHashes:      #see if we already looked at an exact duplicate.
                logging.info("[{:s}] {:s} is an exact duplicate of {:s}. skipping line-by-line checks and removing...".format(time.asctime(),file, hashes.get(hash)))
                lncount= len(f.readlines())
                dupl+= lncount
                f.close()
                #os.remove(input)
                continue
            fileHashes.add(inHash)
            for line in input:      	 #check line-by-line. add uniques to a dict for speedy lookup
                lineCount += 1
                if line in seen: 
                    dupl+=1
                    continue
                seen[line] = 1      	#add entry to dict
                result.append(line)     #add the unique to a list to retain order              
            merge.writelines(result)
    logging.info("[{:s}] completed merge for {:s} in {:f} seconds. found {:d} duplicate lines.".format(time.asctime(),f,time.clock()-begin,dupl))
    threads.remove(threading.currentThread().getName())      

def mergeOperationScheduler(folder1, folder2, maxThreads=5):          #thread to merge all PBs in a given folder
    files1 = set(enumerate_files(folder1))
    files2 = set(enumerate_files(folder2))
    toMerge = files1.intersection(files2)
    singles = files1.union(files2).difference(toMerge)
    print(toMerge)
    if singles:
       for f in singles:
           mkdir_p(args.mergefolder+(getRelativeParents(f)))
           if os.path.isfile(folder1+f):  
               shutil.copy(folder1+f,args.mergefolder+f)
           elif (not(os.path.isfile(folder1+f))):
               shutil.copy(folder2+f,args.mergefolder+f)
           else:
               logging.warn("{:s} not found anywhere.".format(f))
           logging.info("{:s} was not mirrored. file(s) copied to merge directory.".format(f))             
    while toMerge:
        file = toMerge.pop()
        mkdir_p(args.mergefolder+getRelativeParents(file))             #Make a directory to drop into if it isn't already there        
        if (len(threads) < maxThreads): 
            t = threading.Thread(target=mergeOperationThread, args = ([folder1+file,folder2+file],args.mergefolder+file))  
            threads.append(t.getName())
            t.start()
 

parser = argparse.ArgumentParser()
parser.add_argument("--maxthreads", help = "maximum allowed parallel threads", type = int)
parser.add_argument("--folder1", help = "first input directory")
parser.add_argument("--folder2", help = "second input directory")
parser.add_argument("--mergefolder", help = "where to place the output")
parser.add_argument("--mergemode", help = "append or overwrite to merge file. accepts w (write), a (append)")
args = parser.parse_args()
logging.basicConfig(filename = "merge_{:d}.log".format(int(time.time())),level = logging.INFO)

if not ((args.mergemode == "w") or (args.mergemode == "a")):
    args.mergemode = "w"
fileHashes = set()
threads = []
mergeOperationScheduler(args.folder1,args.folder2,args.maxthreads)
