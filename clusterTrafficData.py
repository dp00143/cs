from random import random

__author__ = 'Daniel'
from KMeans import *
from CentroidDetermination import *
from clusterEvaluation import *
import pika
import json
from RingBuffer import RingBuffer
import logging
from pprint import pformat
import csv
from datetime import datetime, timedelta
from customLogging import info, writeToCsv, setChannelName
from trafficDataImport import getMetaData

initialPacketsize = 100
packetsize = 10
centroidinp = [[],[]]
means = False
clusterResult = []
#Here only the features are stored which are taken into account while clustering
clusterDataStore = RingBuffer(50000)
#Here metadata is stored. Basically everything which is important for later analysis but should not taken into
# account while clustering (e.g. location, timestamp) . It must be ensured that this buffer is consistent with the
# buffer above (i.e. index links the data)
# TODO: Once able to retrieve individual values through CKAN API fill this with values
metaDataStore = RingBuffer(50000)


#save the chosen k
k = 0

datetimeFormat = '%Y-%m-%dT%H:%M:%S'
#Timestamps to know when initial centroid computation and the recalculations take place
startdate = datetime.now()
datesincerecalculation = datetime.now()
recalculationtime = 0

def init(name, recalctime):
    global recalculationtime
    # logging.basicConfig(filename="log/"+name+".log",filemode='wb')
    # logger = logging.getLogger(__name__)
    startdate = datetime.now()
    recalculationtime = recalctime
    #setup connection and declare channel
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="127.0.0.1"))
    channel = connection.channel()
    channel.queue_declare(queue=name)
    setChannelName(name)
    #initialize logging
    # logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(message)s')
    ch.setFormatter(formatter)
    # logger.addHandler(ch)
    return channel




#for performance reasons, the for loop in this method is also used to store the values in the Ring Buffer
def transform(arr):
    ret = []
    for i in range(len(arr[0])):
        ret.append([arr[0][i], arr[1][i]])
        clusterDataStore.append(ret[i])
    return ret

def recalculated_clustering(inp, k):
    # global logger
    means = recalculate_centroids(inp, k)
    kmeansinput = transform(inp)
    features = len(kmeansinput[0])
    weights = [1 for i in range(features)]
    result = kmeans(means, kmeansinput, weights, features, len(means))
    return result


def initial_clustering(inp):
    # global logger
    means = predetermine_centroids(inp)
    kmeansinput = transform(inp)
    # pprint(means)
    features = len(kmeansinput[0])
    weights = [1 for i in range(features)]
    result = [kmeans(v, kmeansinput, weights, features, len(v)) for v in means]
    # pprint(result)
    clustering = [c["cluster"] for c in result]
    silhoutteCoefficients = [silhoutteCoefficient(c1) for c1 in clustering]
    clustersizes = [[len(c) for c in c1] for c1 in clustering]
    highestSil = -1
    for i, s in enumerate(silhoutteCoefficients):
        if s>highestSil:
            highestSil = s
            idx = i
    info("Clustersizes after initial Clustering:")
    info(pformat(clustersizes))
    info("Chosen k = %i" % len(clustering[idx]))
    return result[idx]

def callback(ch, method, properties, body):
    global centroidinp, means, clusterResult, k, logger, datetimeFormat, startdate, datesincerecalculation, recalculationtime
    body = json.loads(body)
    currentTimeStamp = datetime.strptime(body["data"]["TIMESTAMP"], datetimeFormat)
    report_id = body["data"]["REPORT_ID"]
    metaData = getMetaData(report_id)
    if not means:
        if (currentTimeStamp-startdate) < timedelta(hours=24):
            centroidinp[0].append(body["data"]["avgSpeed"])
            centroidinp[1].append(body["data"]["vehicleCount"])
            newValue = [body["data"]["avgSpeed"], body["data"]["vehicleCount"]]
            hitBucket = "n/a"
            writeToCsv(newValue, metaData, hitBucket)
            # print "waiting for data %i" %len(centroidinp[0])
            return
        else:
            clusterResult = initial_clustering(centroidinp)
            k = len(clusterResult["means"])
            info("Results after initial clustering:")
            info("Cluster:")
            clustersizes = [len(c) for c in clusterResult['cluster']]
            info(pformat(clustersizes))
            info(pformat(clustersizes))
            means = [{'Average Speed': x[0], 'Vehicle Count': x[1]} for x in clusterResult['means']]
            info("Centroids:")
            info(pformat(means))
            return
    elif (currentTimeStamp-datesincerecalculation) > timedelta(hours=recalculationtime):
        # Enough time has past to recalibrate
        datesincerecalculation = currentTimeStamp
        centroidinp = [[], []]
        newValue = [body["data"]["avgSpeed"], body["data"]["vehicleCount"]]
        clusterDataStore.append(newValue)
        metaDataStore.append(metaData)
        for x in clusterDataStore.get():
            for i, v in enumerate(x):
                centroidinp[i].append(v)
        info("Recalibrating Centroids...")
        clusterResult = recalculated_clustering(centroidinp, k)
        if clusterResult == 0:
            # Some bug which can only be reproduced at random causes clusterResult to become 0
            # Ugly hack: just ignore and hope system recovers...
            while clusterResult == 0:
                clusterResult = recalculated_clustering(centroidinp, k)
            return
        # info(m)
        # info(lastm)
        # info("New Centroids. Last m %i, new m $i" % (lastm, m))
        means = [{'Average Speed': x[0], 'Vehicle Count': x[1]} for x in clusterResult['means']]
        info(pformat(means))
        clustersizes = [len(c) for c in clusterResult['cluster']]
        hitBucket = "n/a"
        info(pformat(clustersizes))
        writeToCsv(newValue, metaData, hitBucket)
        return
    else:
        newValue = [body["data"]["avgSpeed"], body["data"]["vehicleCount"]]
        clusterDataStore.append(newValue)
        metaDataStore.append(metaData)
        features = len(newValue)
        weights = [1 for i in range(features)]
        clusterResult = kmeans_new_value(clusterResult['means'], clusterResult['cluster'], weights, features, newValue)
        means = [{'Average Speed': x[0], 'Vehicle Count': x[1]} for x in clusterResult['means']]
        info("Centroids at time "+currentTimeStamp.strftime(datetimeFormat))
        info(pformat(means))
        clustersizes = [len(c) for c in clusterResult['cluster']]
        hitBucket = clusterResult['hit_bucket']
        info(pformat(clustersizes))
        writeToCsv(newValue, metaData, hitBucket)
        return


def clusterData(channelname, recalctime):
    # global logger
    channel = init(channelname, recalctime)
    info("Started main program, waiting for data...")
    with open(channelname+'Data'+datetime.now().strftime("%Y-%m-%d")+'.csv', 'wb') as csvfile:
        wr = csv.writer(csvfile, delimiter=',', quotechar='\"', quoting=csv.QUOTE_MINIMAL)
        wr.writerow(["Average Speed", "Vehicle Count", "Timestamp", "Street1", "City1", "Latitude1", "Longitude1",
                     "Street2", "City2", "Latitude2", "Longitude2", "Nearest Centroid"])
    channel.basic_consume(callback, queue=channelname, no_ack=True)
    channel.start_consuming()
