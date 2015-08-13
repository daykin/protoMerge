import glob as gl
from os.path import join, isfile
import os


def bar(subfolder1,subfolder2,merge="merge"):

    print files1
    print files2
def foo(folder1,folder2,mergeFolder):
    for subdir in os.listdir(folder1):
        bar(join(folder1,subdir),join(folder2,subdir))
    
    #for folder in gl.glob(join(folder1,"*")):
    #    subdirs.append(folder)
      


foo("app1","app2","merge")
        
         