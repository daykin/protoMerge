#PB merge script

###info:
This script looks recursively into two folders, and merges any redundant protocol buffer (pb) files into another specified folder. It will also copy non-redundant files to the merge directory.
Any two filenames **must** be exactly identical in order to merge; e.g. "1Hz-2015.pb" will not merge with "1Hz-2007.pb". (on *nix platforms, the .pb extension must be uniform case (both upper or both lower) in order to merge. That is, "example.pb" will not merge with "example.PB". YMMV on other platforms.)

For convenience, the Auto-generated EPICSevent_pb2. is provided for un/marshalling the PB data.

###Usage:    
	python protoBufMerger.py [--maxthreads <threads>] [--folder1 <directory>] [--folder2 <directory>] [--mergefolder <directory>] [--mergemode [w|a]]
| command     | what does it do?                                   |
|-------------|----------------------------------------------------|
| maxthreads  | maximum concurrent merge operations. defaults to 5 |
| folder1     | (required) first merge directory; eg. /srv/lts/    |
| folder2     | (required) second merge directory                  |
| mergefolder | (required) output directory                        |
| mergemode   | (w)rite or (a)ppend to existing, defaults to w     |



author: Evan Daykin <daykin@frib.msu.edu>