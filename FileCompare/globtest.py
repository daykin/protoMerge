import glob as gl
import threading
from os.path import join


def removeDuplicates(lines, outFile): 
    seen = {}
    result = []
    dupl = 0
    for item in lines:
        if item in seen: 
            dupl+=1
            continue
        seen[item] = 1
        result.append(item)
    print "removed {:d} duplicates.".format(dupl,cnt)
    return result

def mergefromDirectory(concurrentThreads = 5):
    threads = []
    for folder in gl.glob("20*"):
        filesToMerge = []
        lines = []
        output = open(join(folder,"{:s}_merged.PB".format(folder)),"wb")
        for pb in gl.glob(join(folder,"*.pb")):
            filesToMerge.append(pb)
        for f in filesToMerge:
            with open(f,"rb") as inFile:
                lines.extend(inFile.readlines())
                inFile.close()
        filesToMerge = []
        output.writelines(removeDuplicates(lines))
        output.close()


