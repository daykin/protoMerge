import threading

def mergeWorkerThread(folder):
    files = []
    inputLines = []
    merged = open(join(folder,"{:s}_merged.PB".format(folder)),"wb")
    seen = {}
    result = []
    dupl = 0
    cnt = 0
    for item in lines:
        if item in seen: 
            dupl+=1
            continue
        seen[item] = 1
        result.append(item)
        cnt += 1
    merged.writelines(result)
    print("processed {:d} records with {:d} duplicates.".format(cnt,dupl))
    merged.close()
        


def mergeScheduler(maxThreads=5):    
    folders = []
    for folder in gl.glob("20*"):
        folders.append(folder)
    lines = []
    while folders:          
        #TODO: start a new thread for each folder

        #for pb in gl.glob(join(folder,"*.pb")):
        #    filesToMerge.append(pb)   
        #while(filesToMerge):
            #with open(f,"rb") as inFile:
            #    lines.extend(inFile.readlines())
            #    inFile.close()                      
            if runningThreads <= maxThreads:
                t = (threading.Thread(target=mergeWorkerThread,args=(folder,f)))

threads = []
output.writelines(removeDuplicates(lines))
output.close()
    

