__author__ = 'Daniel'
import datetime
import threading
import historicTrafficImport
import storeData

datetimeFormat = '%Y-%m-%dT%H:%M:%S'
start = datetime.datetime.strptime('2014-02-13T11:30:00', datetimeFormat)


name = "historicTraffic"
t1 = threading.Thread(target=historicTrafficImport.importAllData, args=(start, name))
t2 = threading.Thread(target=storeData.storeData, args=(name, name))


t1.start()
t2.start()