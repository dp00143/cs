__author__ = 'Daniel'

import trafficDataImport
import clusterTrafficData
import threading

ch1 = "traffic"
t1 = threading.Thread(target=trafficDataImport.importAllData, args=(ch1,))
t2 = threading.Thread(target=clusterTrafficData.clusterData, args=(ch1, 6))


print "starting t1"
t2.start()
print "starting t2"
t1.start()
print "both threads started"
