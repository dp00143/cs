__author__ = 'Daniel'
import math
import sys
from pprint import pprint

from scipy.stats import kde
import numpy as np


def pdf_estimation(x):
    density, xgrid, xarr = [], [], []
    for i in range(len(x)):
        density.append((kde.gaussian_kde(x[i])))
        xgrid.append(np.linspace(min(x[i]), max(x[i]), len(x[i])))
        xarr.append(x[i])
    return density,xgrid,xarr

#input data x. Possible number of centroids are computed as the number of turning points (see find_turning_points)
def predetermine_centroids(inp):
    x = []
    for arr in inp:
        x.append([float(v) for v in arr])
    density, xgrid, xarr = pdf_estimation(x)
    # rows = int(math.ceil(len(x)/2.))
    # fig, axs = plt.subplots(rows, 2)
    centroid_nums = set()
    for grid, d in zip(xgrid,density):
        tps = find_turning_points(d(grid))
        centroid_nums.add(len(tps))
    # print "centroid number"
    # pprint(centroid_nums)
    centroids = []
    for num, b in enumerate(centroid_nums):
        ci = [[] for n in range(b)]
        centroids.append(ci)
        betas = []
        for grid, d, xi in zip(xgrid, density, xarr):
            # ax.hist(xi, bins=10, normed=True)
            # ax.plot(grid, d(grid), 'r-')
            betas.append([find_beta(float(i)/float(b+2), d(grid)) for i in range(1,b+2)])
        for i in range(b):
            for j in range(len(betas)):
                idx = int(math.ceil(float((betas[j][i]+betas[j][i+1]))/float(2)))
                try:
                    centroids[num][i].append(x[j][idx])
                except:
                    pprint(num)
                    pprint(i)
                    pprint(j)
                    pprint(idx)
                    pprint(x)
                    pprint(centroids)
        # plt.show()
        # pprint(centroids)
    return centroids

def recalculate_centroids(inp, k):
    x = []
    for arr in inp:
        x.append([float(v) for v in arr])
    density, xgrid, xarr = pdf_estimation(x)
    centroids = [[] for n in range(k)]
    betas = []
    for grid, d, xi in zip(xgrid, density, xarr):
        betas.append([find_beta(float(i)/float(k+2), d(grid)) for i in range(1, k+2)])
    for i in range(k):
        for j in range(len(betas)):
            idx = int(math.ceil(float((betas[j][i+1]))/float(2)))
            centroids[i].append(x[j][idx])
    return centroids

def find_beta(quantile, pdf):
    norm_constant = sum(pdf)
    #if norm_constant == 0:
    #    pprint("pdf:")
    #    pprint(pdf)
    #    pprint("quantile:")
    #    pprint(quantile)
    #    return 0
    # normalize values so they are between 0 and 1
    pdf = [float(x)/float(norm_constant) for x in pdf]
    # print sum(pdf)
    i = 0
    s = 0

    while(quantile > s and i < len(pdf)-1):
        s += pdf[i]
        i += 1
    # print "Summe %f, Index %i" %(s,i)
    return i

#assumption: f is smooth
#input: probability density function f
#output: list of directions
def find_turning_points(f):
    delta_y = 0.001*float(max(f))
    delta_x = 0.1*float(len(f))
    tp = []
    # print delta_x
    # last_x = 0
    direction = False
    for i, x in enumerate(f):
        if i == 0:
            last_x = x
            continue
        if x > last_x+delta_y:
            #start of function
            last_x = x
            if not direction:
                direction = "up"
            # we changed direction -> turning point
            elif direction != "up":
                direction = "up"
            #still going in the same direction; continue
            else:
                continue
        elif x < last_x-delta_y :
            #start of function
            last_x = x
            if not direction:
                direction = "down"
            # we changed direction -> turning point
            elif direction != "down":
                direction = "down"
            #still going in the same direction; continue
            else:
                continue
        else:
            #start of function
            last_x = x
            if not direction:
                direction = "straight"
                first_straight_i = i
                continue
            # we changed direction -> turning point
            elif direction != "straight":
                direction = "straight"
                first_straight_i = i
                continue
            #still going in the same direction; continue
            else:
                if(i-first_straight_i) < delta_x:
                    continue
                else:
                    print(i-first_straight_i)
                    first_straight_i = sys.maxsize
        t = direction
        tp.append(t)
    del_me = []
    for i, t in enumerate(tp):
        if i > 0 and t == tp[i-1]:
            del_me.append(i)
    for d in reversed(del_me):
        del tp[d]
    return tp
