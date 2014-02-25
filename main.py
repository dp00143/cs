__author__ = 'Daniel'

import trafficDataImport
import clusterTrafficData
import threading

t1 = threading.Thread(target=trafficDataImport.importAllData)
t2 = threading.Thread(target=clusterTrafficData.clusterData)

print "starting t1"
t2.start()
print "starting t2"
t1.start()
print "both threads started"