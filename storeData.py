__author__ = 'Daniel'
import csv
import datetime
import json
import logging
import pika
from customLogging import UnicodeWriter


datetimeFormat = '%Y-%m-%dT%H:%M:%S'
channelName = ""
fileCreated = False

def init(name, exchange):
    #setup connection and declare channel
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="127.0.0.1"))
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange, exchange_type='fanout')
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=exchange, queue=queue_name)
    setChannelName(name)
    return channel, queue_name


def callback(ch, method, properties, body):
    global datetimeFormat, startdate
    body = json.loads(body)
    currentTimeStamp = datetime.datetime.strptime(body["data"]["TIMESTAMP"], datetimeFormat)
    print currentTimeStamp
    writeToCsv(body['data'], currentTimeStamp, body["metadata"])

def writeToCsv(newValue, timestamp,metaData):
    global channelName, fileCreated

    if not fileCreated:
        with open(channelName + 'Data' + timestamp.strftime("%Y-%m-%d") + '.csv', 'wb') as csvfile:
            wr = csv.writer(csvfile, delimiter=',', quotechar='\"', quoting=csv.QUOTE_MINIMAL)
            wr.writerow(newValue.keys()+metaData.keys())
        fileCreated = True
    with open(channelName+'Data'+timestamp.strftime("%Y-%m-%d")+'.csv', 'ab') as csvfile:
        wr = UnicodeWriter(csvfile)
        nv = [v for v in newValue.values()]
        md = [v for v in metaData.values()]
        row = nv + md
        wr.writerow(row)

def setChannelName(name):
    global channelName
    channelName = name

def storeData(name, exchange):
    # global logger
    channel, queue_name = init(name, exchange)
    channel.exchange_declare(exchange=exchange, exchange_type='fanout')
    # info(startdate, "Started main program, waiting for data...")
    channel.basic_consume(callback, queue=queue_name, no_ack=True)
    channel.start_consuming()