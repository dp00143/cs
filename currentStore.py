__author__ = 'Daniel'

from dateutil import roundTime
import datetime
import storeData
import threading
import historicTrafficImport


start = (roundTime(datetime.datetime.now(), 60*5) - datetime.timedelta(hours=3)).replace(microsecond=0)
name = "Traffic"
t1 = threading.Thread(target=historicTrafficImport.importAllData, args=(start, name))
t2 = threading.Thread(target=storeData.storeData, args=(name, name))


t1.start()
t2.start()