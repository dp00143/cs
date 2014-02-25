from cmath import sqrt

__author__ = 'Daniel'
from KMeans import dist
import sys


def distanceToCluster(A, o):
    n = len(A)
    if n == 0:
        return 0
    s = 0
    w = [1 for i in range(len(A[0]))]
    for a in A:
        s += dist(a, o, w)
    s /= n
    return s


def silhouette(A, nearestDist, o):
    a = distanceToCluster(A, o)
    if max(a,nearestDist) ==0:
        return 0
    return (nearestDist - a) / max(a, nearestDist)


def distanceToNearestCluster(i, C, o):
    m = sys.maxsize
    for j, c in enumerate(C):
        if j == i:
            continue
        d = distanceToCluster(c, o)
        if d < m:
            m = d
    return m


#TODO: make this efficient by precomputing distance between each point to each cluster and storing in matrix
def silhoutteCoefficient(C):
    n = len(C)
    s = [0 for i in range(len(C))]
    for i, c1 in enumerate(C):
        for o in c1:
            nearest = distanceToNearestCluster(i, C, o)
            s[i] += silhouette(c1, nearest, o)
        s[i] /= n
    return s

def variances(result):
    variances = []
    for l, res in enumerate(result):
        variances.append([])
        for cluster in res["cluster"]:
            n = len(cluster)
            v = 0
            for i in range(n):
                for j in range(n):
                    v += 1. / 2. * (dist(cluster[i], cluster[j], [1, 1, 1, 1]) ** 2)
            v /= n ** 2
            variances[l].append(v)
    measure = {"variance": variances, "standard_deviation": [[sqrt(v) for v in variance] for variance in variances]}
    return measure

    # x = [0, 0, 0]
    # for i, var in enumerate(measure["standard_deviation"]):
    #     for v in var:
    #         x[i] += v
    #     x[i] /= len(var)
    # pprint(x)


# result = kmeans(init_means, inp, weights,4)
#
# for i in range(1, 1000):
#     if not result:
#         break
#     result = kmeans(result['means'], inp, weights,4)
#
#
# if result:
#     for cluster in enumerate(result["cluster"]):
#         class1, class2, class3 = 0, 0, 0
#         for v in cluster[1]:
#             if data["class"][v] == "Iris-setosa":
#                 class1 += 1
#             elif data["class"][v] == "Iris-versicolor":
#                 class2 += 1
#             elif data["class"][v] == "Iris-virginica":
#                 class3 += 1
#         if cluster[1]:
#             if class1 >= class2:
#                 if class2 >= class3:
#                     print "Cluster %i resembles most closely class Iris-setosa. Elements = % i, Precision = %f, Recall = %f" %(cluster[0],len(cluster[1]),
#                     ((float(class2)+float(class3))/(float(class1)+float(class2)+float(class3))), float(class1)/50.)
#                 elif class3 >= class1:
#                     print "Cluster %i resembles most closely class Iris-virginica. Elements = % i,  Precision %f, Recall = %f" %(cluster[0],len(cluster[1]),
#                     ((float(class2)+float(class1))/(float(class1)+float(class2)+float(class3))), float(class3)/50.)
#             elif class2 >= class3:
#                 print "Cluster %i resembles most closely class Iris-versicolor. Elements = % i,  Precision = %f, Recall = %f" %(cluster[0],len(cluster[1]),
#                     ((float(class1)+float(class3))/(float(class1)+float(class2)+float(class3))), float(class2)/50.)
#             else:
#                 print "Cluster %i resembles most closely class Iris-virginica. Elements = % i,  Precision = %f, Recall = %f" %(cluster[0],len(cluster[1]),
#                 ((float(class2)+float(class1))/(float(class1)+float(class2)+float(class3))), float(class3)/50.)
#         else:
#             print "No elements in cluster %i" %(cluster[0])


