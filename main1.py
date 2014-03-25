__author__ = 'Daniel'

import clusterTrafficData
import threading

ch1 = "traffic2"
# t1 = threading.Thread(target=trafficDataImport.importAllData, args=())
t2 = threading.Thread(target=clusterTrafficData.clusterData, args=(ch1, 3))


print "starting cluster for 2"
t2.start()
# print "starting t2"
# t1.start()
# print "both threads started"
