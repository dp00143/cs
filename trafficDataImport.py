__author__ = 'Daniel'
import urllib2
import urllib
import json
from pprint import pprint
from httplib2 import Http
from unicodedata import normalize
from datetime import datetime
import pika
from time import sleep, time

#specify hostname by name or ip adress
def establishConnection(hostname='127.0.0.1'):
    connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
    return connection


# ,"limit":2
def importData(url, resource, limit=10000):
    data_string = urllib.quote(json.dumps({'resource_id': resource, 'limit': limit}))
    response = urllib2.urlopen(url, data_string)
    assert response.code == 200

    response_dict = json.loads(response.read())
    assert response_dict['success'] is True
    result = response_dict['result']
    # pprint(result)
    return result["records"]

lastTimeStamp = False
datetimeFormat = '%Y-%m-%dT%H:%M:%S'

#msgType store, forward or transform
def wrapAndSendInitialData(inp, msgTypes, connection, channelName):
    global lastTimeStamp
    channel = connection.channel()
    channel.queue_declare(queue=channelName)
    for arr in inp:
        data = {}
        data["type"] = msgTypes
        data["data"] = arr
        jsonData = json.dumps(data)
        channel.basic_publish(exchange='', routing_key='traffic', body=jsonData)
    lastTimeStamp = datetime.strptime(data["data"]["TIMESTAMP"], datetimeFormat)

def wrapAndSendData(inp, msgTypes, connection, channelName):
    global lastTimeStamp
    channel = connection.channel()
    channel.queue_declare(queue=channelName)
    timestamp = str(datetime.now())
    for arr in inp:
        data = {}
        data["type"] = msgTypes
        data["data"] = arr
        jsonData = json.dumps(data)
        #only publish if data is new
        currentTimeStamp = datetime.strptime(data["data"]["TIMESTAMP"], datetimeFormat)
        sentData = currentTimeStamp > lastTimeStamp
        if sentData:
            channel.basic_publish(exchange='', routing_key='traffic', body=jsonData)
    lastTimeStamp = datetime.strptime(data["data"]["TIMESTAMP"], datetimeFormat)
    return sentData

def importAllData():
    connection = establishConnection()
    url = "http://ckan.projects.cavi.dk/api/action/datastore_search"
    resourceValues = "d7e6c54f-dc2a-4fae-9f2a-b036c804837d"
    resourceMetaData = "e132d528-a8a2-4e49-b828-f8f0bb687716"
    values = importData(url, resourceValues)
    trafficMetaData = importData(url, resourceMetaData)
    types = ['cluster']
    print "publish initial data of size %i" % len(values)
    wrapAndSendInitialData(values, types, connection, 'traffic')
    #give the CKAN archive time to update data
    sleep(300)
    while True:
        #fetch newest 10 samplesquit(9
        values = importData(url, resourceValues,10)
        if wrapAndSendData(values,types,connection,'traffic'):
            time = datetime.now()
            print str(time)+":published new data"
        #give the CKAN archive time to update data
        sleep(300)
    connection.close()
    return

# main()
