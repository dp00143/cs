t__author__ = 'Daniel'
from httplib2.socks import HTTPError
import urllib2
import urllib
import json
from pprint import pprint
import datetime
import pika
from dateutil import roundTime

#specify hostname by name or ip adress
def establishConnection(hostname='127.0.0.1'):
    connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
    return connection


# ,"limit":2
def importData(url, resource, date, limit=500,):
    # url = "http://www.gatesense.com/ckan/api/action/datastore_search?resource_id=3c2200b6-3d8e-4707-9105-a755680f921a" \
    #       "&filters={\"TIMESTAMP\":\"2014-02-13\"}"
    date = str(date)
    url = "http://www.gatesense.com/ckan/api/action/datastore_search"
    data_string = urllib.quote(json.dumps({'resource_id': resource, 'limit': limit,
                                           # 'filter':{"REPORT_ID":"203822"}}))
                                           'filters':{"TIMESTAMP":date}}))
    while True:
        try:
            response = urllib2.urlopen(url, data_string)
            break
        except: 
            print "Failure on url open, try again"
        
    assert response.code == 200

    response_dict = json.loads(response.read())
    assert response_dict['success'] is True
    result = response_dict['result']
    # pprint(result)
    return result["records"]

lastTimeStamp = False
datetimeFormat = '%Y-%m-%dT%H:%M:%S'
programRunning = True

message_sent_counter = 0
#msgType store, forward or transform
def wrapAndSendData(inp, msgTypes, connection, exchange):
    global lastTimeStamp, message_sent_counter
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange, type="fanout")
    for arr in inp:
        data = None
        while True:
            try:
                metadata = getMetaData(arr[u'REPORT_ID'])
                break
            except:
                print "metaData retrieval timed out, try again"
        data = {"type": msgTypes, "data": arr, "metadata": metadata}
        jsonData = json.dumps(data)
        channel.basic_publish(exchange=exchange, routing_key='', body=jsonData)
        message_sent_counter += 1
        if message_sent_counter % 100 == True:
            connection.process_data_events()
        if data is not None:
            lastTimeStamp = datetime.datetime.strptime(data["data"][u"TIMESTAMP"], datetimeFormat)


def importAllData(start, exchange):
    connection = establishConnection()
    # url = "http://ckan.projects.cavi.dk/api/action/datastore_search"
    url = "http://www.gatesense.com/ckan/api/action/datastore_search"
    # resourceValues = "d7e6c54f-dc2a-4fae-9f2a-b036c804837d"
    resourceValues = "3c2200b6-3d8e-4707-9105-a755680f921a"

    startdate = start
    enddate = datetime.datetime.now()
    types = ['cluster']
    while startdate<enddate:
        wrapAndSendData((importData(url,resourceValues,startdate)), types, connection, exchange)
        pprint(startdate)
        startdate += datetime.timedelta(minutes=5)
        enddate = (roundTime(datetime.datetime.now(), 60*5) - datetime.timedelta(minutes=15)).replace(microsecond=0)
        connection.sleep(20)
    while True:
        date = (roundTime(datetime.datetime.now(), 60*5) - datetime.timedelta(hours=3)).replace(microsecond=0)
        newData = (importData(url,resourceValues,date))
        if len(newData)==0:
            connection.sleep(60*5)
            continue
        wrapAndSendData(newData, types, connection, exchange)
        connection.sleep(60*5)


def getMetaData(reportid):
    url = "http://www.gatesense.com/ckan/api/action/datastore_search?resource_id=fdad4220-4d01-4541-8959-35cc539042a7" \
          "&filters={%22REPORT_ID%22:"+str(reportid)+"}"
    response = urllib2.urlopen(url)
    assert response.code == 200
    response_dict = json.loads(response.read())
    # assert response_dict[u'result'] is True
    result =response_dict[u'result']
    dict = result[u"records"][0]
    # meta_data = []
    # for k, v in dict.iteritems():
    #     meta_data.append(v)
    return dict



def shutDown():
    global programRunning
    programRunning = False

# importAllData()
# pprint(getMetaData(158324))
# values = importData("","3c2200b6-3d8e-4707-9105-a755680f921a")
# pprint(len(values))
# pprint(values[0])
# pprint(values[len(values)-1])
