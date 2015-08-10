#!/usr/bin/env python
'''Gets some data from the server using the PB raw protocol and then prints out some information from each line.
This is an example of how to process the PB/HTTP protocol in python
This is not the most efficient but should serve as an example.
'''

from urllib import urlencode
from urllib2 import urlopen

import EPICSEvent_pb2 as pb

#import curses.ascii as ascii

#url = '%s?%s'%(baseurl, urlencode(Q))

#print 'Will get',url

#R = urlopen(url)
#print 'Code',R.getcode()
#print 'Headers',R.info()

#lines = R.readlines()
lines = []
with open("C2014.pb", 'rb') as f:
    lines = f.readlines()

# The PB file format escapes new lines using the escape character so we need to unescape.
escapeTo = [str(unichr(27)), str(unichr(10)), str(unichr(13))]

expectHeader = True
for line in lines:
    line = line.strip()
    if not line:
        expectHeader = True
        continue

    foundEscape = False
    unescapedline = ""
    for char in line:
        if ord(char) == 27: 
           # print "Found escape"
            foundEscape = True
        else:
            if foundEscape:
                foundEscape = False
                unescapedline = unescapedline + escapeTo[ord(char)-1]
                # print "Adding escape for ",  ord(char) 
            else:
                # print "Adding regular char ", ord(char) 
                unescapedline = unescapedline + char

    # print ':'.join(x.encode('hex') for x in line) 
    # print ':'.join(x.encode('hex') for x in unescapedline) 
    if expectHeader:
        I = pb.PayloadInfo()
        I.ParseFromString(unescapedline)
        print "Processed header for ", I.pvname, " for year ", I.year 
        expectHeader = False
    else:
        d = pb.ScalarDouble()
        d.ParseFromString(unescapedline)
        print "Processed value with secondsintoyear", d.secondsintoyear, "and nanos", d.nano, "and val", d.val, "and severity", d.severity, " and status", d.status