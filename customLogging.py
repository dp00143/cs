__author__ = 'Daniel'
from datetime import datetime
import csv, codecs, cStringIO
from pprint import pprint

channelname = ""

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, delimiter=',', quotechar='\"', quoting=csv.QUOTE_MINIMAL, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") if s is not None else '' for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def writeToCsv(newValue, metaData, hitbucket):
    with open(channelname+'Data'+datetime.now().strftime("%Y-%m-%d")+'.csv', 'ab') as csvfile:
        wr = UnicodeWriter(csvfile)
        wr.writerow([str(newValue[0]), str(newValue[1]), metaData[0], metaData[1], metaData[2],
                     metaData[3], metaData[4], metaData[5], metaData[6], metaData[7], str(hitbucket)])

def info(msg):
    global channelname
    logname = channelname+'Log'+datetime.now().strftime("%Y-%m-%d")+".txt"
    logfile = open(logname, 'ab')
    logfile.write(msg+"\n")

def setChannelName(name):
    global channelname
    channelname = name
