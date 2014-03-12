__author__ = 'Daniel'
from KMeans import dist
import sys
from pprint import pformat
from customLogging import info

def distanceToCluster(A, o):
    n = len(A)
    if n == 0:
        return 0
    s = float(0)
    w = [1 for i in range(len(A[0]))]
    for a in A:
        s += float(dist(a, o, w))
    s /= float(n)
    return s


def silhouette(A, nearestDist, o):
    a = float(distanceToCluster(A, o))
    if max(a, nearestDist) == 0:
        return 0
    sil = float((nearestDist - a)) / float(max(a, nearestDist))
    return sil


def distanceToNearestCluster(i, C, o):
    m = sys.maxsize
    for j, c in enumerate(C):
        if j == i:
            continue
        d = float(distanceToCluster(c, o))
        if d < m:
            m = d
    return m


#TODO: make this efficient by precomputing distance between each point to each cluster and storing in matrix
def silhoutteCoefficient(C):
    lengths = [float(len(c)) for c in C]
    totalLength = float(sum(lengths))
    s = [float(0) for i in range(len(C))]
    total = float(0)
    for i, c1 in enumerate(C):
        for o in c1:
            nearest = float(distanceToNearestCluster(i, C, o))
            curSil = float(silhouette(c1, nearest, o))
            s[i] += curSil
            total += curSil
        s[i] /= lengths[i]
    info("Average Silhouette of each Cluster (k = %i): " % (len(C)))
    info(pformat(s))
    info("Average Silhouette of entire Dataset: ")
    sed = total/totalLength
    info(str(sed))
    return sed
