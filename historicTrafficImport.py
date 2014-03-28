__author__ = 'Daniel'
from httplib2.socks import HTTPError
import urllib2
import urllib
import json
from pprint import pprint
from httplib2 import Http
from unicodedata import normalize
from datetime import datetime, timedelta
import pika
from time import sleep, time

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
    response = urllib2.urlopen(url, data_string)
    assert response.code == 200

    response_dict = json.loads(response.read())
    assert response_dict['success'] is True
    result = response_dict['result']
    # pprint(result)
    return result["records"]

lastTimeStamp = False
datetimeFormat = '%Y-%m-%dT%H:%M:%S'
programRunning = True

#msgType store, forward or transform
def wrapAndSendData(inp, msgTypes, connection):
    global lastTimeStamp
    channel = connection.channel()
    channel.exchange_declare(exchange='clustertraffic', type="fanout")
    data = None
    for arr in inp:
        data = {"type": msgTypes, "data": arr}
        jsonData = json.dumps(data)
        channel.basic_publish(exchange='clustertraffic', routing_key='', body=jsonData)
    if data is not None:
        lastTimeStamp = datetime.strptime(data["data"]["TIMESTAMP"], datetimeFormat)


def importAllData():
    connection = establishConnection()
    # url = "http://ckan.projects.cavi.dk/api/action/datastore_search"
    url = "http://www.gatesense.com/ckan/api/action/datastore_search"
    # resourceValues = "d7e6c54f-dc2a-4fae-9f2a-b036c804837d"
    resourceValues = "3c2200b6-3d8e-4707-9105-a755680f921a"

    startdate = datetime.strptime('2014-02-13T08:20:00', datetimeFormat)
    enddate = datetime.strptime('2014-03-13T23:55:00', datetimeFormat)
    types = ['cluster']
    while startdate<enddate:
        wrapAndSendData((importData(url,resourceValues,startdate)), types, connection)
        startdate += timedelta(minutes=5)
        sleep(50)
        pprint(startdate)

def getMetaData(reportid):
    # resource_id = "e132d528-a8a2-4e49-b828-f8f0bb687716"
    # url = "http://ckan.projects.cavi.dk/api/action/datastore_search_sql?sql=select%20%22POINT_1_STREET%22," \
    #       "%22POINT_1_CITY%22,%22POINT_1_LAT%22,%22POINT_1_LNG%22,%22POINT_2_STREET%22,%22POINT_2_CITY%22," \
    #       "%22POINT_2_LAT%22,%22POINT_2_LNG%22%20from%20%22" + resource_id + "%22%20where%20%22REPORT_ID%22=" \
    #       + str(reportid)
    resource_id = "fdad4220-4d01-4541-8959-35cc539042a7"
    url = "http://www.gatesense.com/ckan/api/action/datastore_search?resource_id=fdad4220-4d01-4541-8959-35cc539042a7" \
          "&filters={%22REPORT_ID%22:"+str(reportid)+"}"
    # pprint(url)
    response = urllib2.urlopen(url)
    assert response.code == 200
    response_dict = json.loads(response.read())
    # assert response_dict[u'result'] is True
    result =response_dict[u'result']
    dict = result[u"records"][0]
    meta_data = []
    for k, v in dict.iteritems():
        meta_data.append(v)
    return meta_data


def shutDown():
    global programRunning
    programRunning = False

importAllData()
# pprint(getMetaData(158324))
# values = importData("","3c2200b6-3d8e-4707-9105-a755680f921a")
# pprint(len(values))
# pprint(values[0])
# pprint(values[len(values)-1])
