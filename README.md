#PB merge script

###info:
This script looks recursively into two folders, and merges any redundant proto-

col buffer (pb) files into another specified folder. 

any two filenames **must** be exactly identical in order to merge;

e.g. "1Hz-2015.pb" will not merge with "1Hz-2007.pb".

(on *nix platforms, the .pb extension must be uniform case (both upper 

or both lower) in order to merge. YMMV on Windows.)

###Usage:    
	python protoBufMerger.py [--maxthreads] [--folder1 <directory>] [--folder2 <directory>] [--mergefolder <directory>] [--mergemode [w|a]]

| maxthreads  | maximum concurrent merge operations. defaults to 5 |
|-------------|----------------------------------------------------|
| folder1     | (required) first merge directory; eg. /srv/lts/    |
| folder2     | (required) second merge directory                  |
| mergefolder | (required) output directory                        |
| mergemode   | (w)rite or (a)ppend to existing, defaults to w     |



author: Evan Daykin <daykin@frib.msu.edu>