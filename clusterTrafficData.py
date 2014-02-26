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
from datetime import datetime

logger = 0

def init(name):
    global logger
    logging.basicConfig(filename="log/"+name+".log",filemode='wb')
    logger = logging.getLogger(__name__)
    #setup connection and declare channel
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="127.0.0.1"))
    channel = connection.channel()
    channel.queue_declare(queue=name)

    #initialize logging
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return channel


initialPacketsize = 100
packetsize = 10
centroidinp = [[],[]]
means = False
clusterResult = []
#Here only the features are stored which are taken into account while clustering
clusterDataStore = RingBuffer(5000)
#Here metadata is stored. Basically everything which is important for later analysis but should not taken into
# account while clustering (e.g. location, timestamp) . It must be ensured that this buffer is consistent with the
# buffer above (i.e. index links the data)
# TODO: Once able to retrieve individual values through CKAN API fill this with values
metaDataStore = RingBuffer(5000)

#Denotes the number of steps after which recalculation of k takes place
m = 0
maxm = 500

#save the chosen k
k = 0

datetimeFormat = '%Y-%m-%dT%H:%M:%S'




#for performance reasons, the for loop in this method is also used to store the values in the Ring Buffer
def transform(arr):
    ret = []
    for i in range(len(arr[0])):
        ret.append([arr[0][i], arr[1][i]])
        clusterDataStore.append(ret[i])
    return ret

def recalculated_clustering(inp, k):
    global logger
    means = recalculate_centroids(inp, k)
    kmeansinput = transform(inp)
    features = len(kmeansinput[0])
    weights = [1 for i in range(features)]
    result = kmeans(means, kmeansinput, weights, features, len(means))
    return result


def initial_clustering(inp):
    global logger
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
    logger.info("Silhoutte metric after initial clustering:")
    logger.info(pformat(clustersizes))
    logger.info(pformat(silhoutteCoefficients))
    mins = sys.maxsize
    for i, s in enumerate(silhoutteCoefficients):
        smean = 0
        for x in s:
            smean += x
        smean /= len(s)
        if smean < mins:
            mins = smean
            idx = i
    return result[i]

def fakeStreet():
    streets = ["High Street", "North Street", "South Street", "West Street", "East Street"]
    if(random()<0.2):
        return streets[0]
    if(random()<0.4):
        return streets[1]
    if(random()<0.6):
        return streets[2]
    if(random()<0.8):
        return streets[3]
    else:
        return streets[4]
def callback(ch, method, properties, body):
    global centroidinp, means, clusterResult, m, k, logger, datetimeFormat
    if m > maxm:
        lastm = m
        m = 0
        centroidinp = [[], []]
        body = json.loads(body)
        newValue = [body["data"]["avgSpeed"], body["data"]["vehicleCount"]]
        clusterDataStore.append(newValue)
        lastTimeStamp = datetime.strptime(body["data"]["TIMESTAMP"], datetimeFormat)
        street = fakeStreet()
        metaData = [lastTimeStamp, street]
        metaDataStore.append(metaData)
        for x in clusterDataStore.get():
            for i, v in enumerate(x):
                centroidinp[i].append(v)
        logger.info("Recalcalibrating Centroids...")
        clusterResult = recalculated_clustering(centroidinp, k)
        if clusterResult == 0:
            # Some bug which can only be reproduced at random causes clusterResult to become 0
            # Ugly hack: just ignore and hope system recovers...
            while(clusterResult == 0):
                clusterResult = recalculated_clustering(centroidinp, k)
            return
        # logger.info(m)
        # logger.info(lastm)
        # logger.info("New Centroids. Last m %i, new m $i" % (lastm, m))
        means = [{'Average Speed': x[0], 'Vehicle Count': x[1]} for x in clusterResult['means']]
        logger.info(pformat(means))
        clustersizes = [len(c) for c in clusterResult['cluster']]
        hitBucket = "n/a"
        logger.info(pformat(clustersizes))
        writeToCsv(newValue, metaData, hitBucket)
        return
    if not means:
        m = 0
        if len(centroidinp[0]) < initialPacketsize:
            body = json.loads(body)
            centroidinp[0].append(body["data"]["avgSpeed"])
            centroidinp[1].append(body["data"]["vehicleCount"])
            newValue = [body["data"]["avgSpeed"], body["data"]["vehicleCount"]]
            lastTimeStamp = datetime.strptime(body["data"]["TIMESTAMP"], datetimeFormat)
            street = fakeStreet()
            metaData = [lastTimeStamp, street]
            hitBucket = "n/a"
            writeToCsv(newValue, metaData, hitBucket)
            # print "waiting for data %i" %len(centroidinp[0])
            return
        else:
            clusterResult = initial_clustering(centroidinp)
            k = len(clusterResult["means"])
            logger.info("Results after initial clustering:")
            logger.info("Cluster:")
            clustersizes = [len(c) for c in clusterResult['cluster']]
            logger.info(pformat(clustersizes))
            logger.info(pformat(clustersizes))
            means = [{'Average Speed': x[0], 'Vehicle Count': x[1]} for x in clusterResult['means']]
            logger.info("Centroids:")
            logger.info(pformat(means))
    else:
        m += 1
        body = json.loads(body)
        newValue = [body["data"]["avgSpeed"], body["data"]["vehicleCount"]]
        clusterDataStore.append(newValue)
        lastTimeStamp = datetime.strptime(body["data"]["TIMESTAMP"], datetimeFormat)
        street = fakeStreet()
        metaData = [lastTimeStamp, street]
        metaDataStore.append(metaData)
        features = len(newValue)
        weights = [1 for i in range(features)]
        clusterResult = kmeans_new_value(clusterResult['means'], clusterResult['cluster'], weights, features, newValue)
        means = [{'Average Speed': x[0], 'Vehicle Count': x[1]} for x in clusterResult['means']]
        logger.info("Centroids:")
        logger.info(pformat(means))
        logger.info("Cluster Step %i:" % m)
        clustersizes = [len(c) for c in clusterResult['cluster']]
        hitBucket = clusterResult['hit_bucket']
        logger.info(pformat(clustersizes))
        writeToCsv(newValue, metaData, hitBucket)
        return

def writeToCsv(newValue, metaData, hitbucket):
    with open('trafficData.csv', 'ab') as csvfile:
        wr = csv.writer(csvfile, delimiter=',', quotechar='\"', quoting=csv.QUOTE_MINIMAL)
        wr.writerow([newValue[0], newValue[1], metaData[0], metaData[1], hitbucket])

def info(msg):
    logfile = open('trafficLog.txt', 'ab')
    logfile.write(msg)


def clusterData():
    global logger
    channel = init('traffic')
    logger.info("Started main program, waiting for data...")
    with open('trafficData.csv', 'ab') as csvfile:
        wr = csv.writer(csvfile, delimiter=',', quotechar='\"', quoting=csv.QUOTE_MINIMAL)
        wr.writerow(["Average Speed", "Vehicle Count", "Timestamp", "Street", "Nearest Centroid"])
    channel.basic_consume(callback, queue='traffic', no_ack=True)
    channel.start_consuming()

# main()

#get iris data set / generated data
# data = pandas.read_csv("C:\Dropbox\Surrey\DODO_source\Data\iris.csv")
# length = len(data["sepal_length"])
#
# centroidinp = [data["sepal_length"][:149], data["sepal_width"][:149], data["petal_length"][:149], data["petal_width"][:149]]
# inp = [[data["sepal_length"][i], data["sepal_width"][i], data["petal_length"][i], data["petal_width"][i]]
#        for i in range(length-1)]
#
# interval2 = lambda x: (x+2)*2*pi if (x-0.5>0) else (x-2)*2*pi
# fx2 = lambda x: cos(x)
# fy2 = lambda x: x
#
# data2 = generate_from_skeleton(fx2,fy2,interval2,1000,0.0005,0.0005)
# centroidinp1 = []
# xarr = [x for x in data2["x"]]
# yarr = [y for y in data2["y"]]
# centroidinp1.append(xarr)
# centroidinp1.append(yarr)
# inp1 = [[x,y] for x,y in zip(data2["x"],data2["y"])]
# pprint(init_means)

    # init_means = [[] for i in range(maxk)]
    # for v in enumerate(init_means):
    #     r = randint(0, len(data["sepal_length"]))
    #     init_means[v[0]] = [data["sepal_length"][r], data["sepal_width"][r], data["petal_length"][r], data["petal_width"][r]]
