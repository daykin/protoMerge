import logging
import hashlib
import threading
import sys
import os, shutil
from os.path import join
import time
import argparse

def getFileHash(f):                  #return the hash of an entire file.
    m = hashlib.md5()
    for line in f:
        m.update(line.strip())
    f.seek(0)
    print(m.hexdigest())
    return m.hexdigest()

def mergeOperationThread(files, mergeFile):    
    seen = {}
    result = []
    dupl = 0
    lineCount = 0
    begin = time.clock()
    with open(mergeFile,args.mergemode+"+b") as merge:
        for f in files:
            with open(f,"rb") as infile:
                inHash = getFileHash(infile)
                if inHash in fileHashes:      #see if we already looked at an exact duplicate.
                    logging.info("[{:s}] {:s} is an exact duplicate of {:s}. skipping line-by-line checks ..."\
                                 .format(time.asctime(),f, fileHashes.get(inHash)))
                    lncount= len(infile.readlines())
                    dupl+= lncount
                    infile.close()
                    #os.remove(file)
                    continue
                fileHashes[inHash] = f
                if os.stat(mergeFile).st_size != 0:  
                    infile.readline()    #throw out the header if the merge file already has something
                for line in infile:      #check line-by-line. add uniques to a dict for speedy lookup
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
    files1 = set(filename for filename in os.listdir(subfolder1))
    files2 = set(filename for filename in os.listdir(subfolder2))
    basename = os.path.basename(subfolder1)                                 #Master already made sure this exists in both subfolders 
    #basename2 = os.path.basename(subfolder2)
    toMerge = files1.intersection(files2)
    singles = files1.union(files2).difference(toMerge)
    if singles:
        for filename in singles:
            shutil.copy(filename,join(args.mergefolder,basename))
            logging.info("{:s} is not mirrored. file copied to merge directory.".format(filename))             
    while toMerge:
        file = toMerge.pop()
        mergedir = join(args.mergefolder,basename)
        if not os.path.exists(mergedir):
            logging.info("[{:s}]supplied merge path doesn't exist. creating it.".format(time.asctime))
            os.makedirs(mergedir)           
        if (len(threads) < maxThreads): 
            t = threading.Thread(target=mergeOperationThread, args = ([join(subfolder1,file),join(subfolder2,file)],join(mergedir,file)))  
            threads.append(t.getName())
            t.start()

def mergeMaster(folder1 = os.path.join(os.getcwd(),"appl0"), folder2 = os.path.join(os.getcwd(),"appl1")):        
    subdirs1 = set(os.path.join(folder1,subdir) for subdir in os.listdir(folder1))
    subdirs2 = set(os.path.join(folder1,subdir) for subdir in os.listdir(folder2))
    common = subdirs1.intersection(subdirs2)                    
    singles = subdirs1.union(subdirs2).difference(common)   #subdirs that exist in 1 xor 2
    for subdir in singles:                                  #take care of the singles
        logging.info("{:s} is not mirrored. copying to merged.".format(subdir))
        try:
            shutil.copy(subdir, args.mergefolder)
        except Exception, e:
            logging.warning(e)
    for subdir in common:                                   #merge the mirrored files
        mergeOperationScheduler(join(folder1,subdir),join(folder2,subdir),args.maxthreads)

parser = argparse.ArgumentParser()
parser.add_argument("--maxthreads", help = "maximum allowed parallel threads", type = int)
parser.add_argument("--folder1", help = "first input directory")
parser.add_argument("--folder2", help = "second input directory")
parser.add_argument("--mergefolder", help = "where to place the output")
parser.add_argument("--mergemode", help = "append or overwrite to merge file. accepts w (write), a (append)")
args = parser.parse_args()
                       
logging.basicConfig(filename = "merge_{:d}.log".format(int(time.time())),level = logging.INFO)

if not(args.mergemode == "w" or args.mergemode == "a"):
    args.mergemode = "w"
fileHashes = {}
threads = []
mergeMaster(args.folder1,args.folder2)

