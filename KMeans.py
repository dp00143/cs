from __future__ import division
import sys


def dist(a1, a2, w):
    if(len(a1) != len(a2)):
        print "a1 %i and a1 %i do not have the same size" %(len(a1), len(a2))
        return False
    d = float(0)
    for x in enumerate(a1):
        d += float(abs((x[1]-a2[x[0]])*w[x[0]])**2)
    return d

def density_sensitive_dist(a1,a2,w,p):
    if(len(a1) != len(a2)):
        print "a1 %i and a1 %i do not have the same size" %(len(a1), len(a2))
        return False
    d = dist(a1, a2, w)
    d += (p**d) - 1
    return d


#weight is value between 0 and 1 (1 means maximum value, 0 means it does not weigh in on the distance at all)
def kmeans(means, inp, weights, features, k):
    param = 0.01
    cluster = [[] for i in range(k)]
    for v in enumerate(inp):
        x = v[1]
        closest_k = float(0)
        smallest_error = sys.maxsize
        for cent in enumerate(means):
            d = dist(x, cent[1], weights)
            if not d:
                return d
            error = abs(d)
            if error < smallest_error:
                smallest_error = error
                closest_k = cent[0]
        means[closest_k] = [float(means[closest_k][i]*(1-param)+x[i]*param) for i in range(features)]
        cluster[closest_k].append(v[1])
    return {'means': means, 'cluster': cluster}

def kmeans_new_value(means, cluster, weights, features, value):
    param = 0.01
    closest_k = float(0)
    smallest_error = sys.maxsize
    for cent in enumerate(means):
        d = dist(value, cent[1], weights)
        if not d:
            return d
        error = abs(d)
        if error < smallest_error:
            smallest_error = error
            closest_k = cent[0]
    means[closest_k] = [float(means[closest_k][i]*(1-param)+value[i]*param) for i in range(features)]
    cluster[closest_k].append(value)
    return {'means': means, 'cluster': cluster, 'hit_bucket': closest_k}

