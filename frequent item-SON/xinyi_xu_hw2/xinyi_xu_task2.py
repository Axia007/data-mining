from pyspark import SparkContext
import json
import time
import sys
import math
import collections
import itertools
import numpy as np

time_start = time.time()

def phase1(iterator):
    tempDict = collections.defaultdict(lambda: 0)
    frequentList = {}
    frequent = {}
    result = []
    basket = []
    for item in iterator:
        a = item
        basket.append(sorted(a))
        for item2 in item:
            tempDict[item2] += 1
    # print('---',tempDict)
    for item in tempDict:
        if (tempDict[item] >= perPartS):
            # frequentList.add(item)
            frequent[item] = 1
    frequentList[1] = set(frequent)
    # print('============',len(frequentList[1]))
    maxTime = len(frequentList[1])

    #pair
    frequent = {}
    tempDict = collections.defaultdict(lambda: 0)
    for i in range(0, len(basket)):
        fresingle = [s for s in basket[i] if s in frequentList[1]]
        # print(fresingle)
        mayfrepair = itertools.combinations(fresingle, 2)
        basket[i] = [{}, 1]
        for item in mayfrepair:
            basket[i][0][item] = 1
            tempDict[item] += 1
    for item in tempDict:
        if (tempDict[item] >= perPartS and item[0]!= item[1]):
            # frequentList.add(item)
            frequent[item] = 1
    # print(frequentList)
    frequentList[2] = set(frequent)
    # print("----", frequentList[2])
    point = 3
    while(point <= maxTime):
        # print('point',point)
        frequent = {}
        tempDict = collections.defaultdict(lambda: 0)
        for i in range(0, len(basket)):
            if(basket[i][1] == 0):
                continue
            frequentP = [s for s in basket[i][0] if s in frequentList[point-1]]
            # print('frequentP',frequentP)
            if(len(frequentP) < point):
                basket[i][1] = 0
                continue
            basket[i][0] = {}
            b = set()
            for j in range(0, len(frequentP)):
                for k in range(j,len(frequentP)):
                    a = set(frequentP[j]).union(set(frequentP[k]))
                    if(len(a)== point):
                        b.add(tuple(sorted(a)))
            # print('b',b)
            # b = set(sorted(b))
            for item in b:
                flag = 1
                for item2 in itertools.combinations(item, point - 1):
                    if not(item2 in frequentList[point-1]):
                        flag = 0
                        break
                if(flag == 1):
                    basket[i][0][item] = 1
            for item in b:
                basket[i][0][item] = 1
            # basket[i][0] = b
            # print('basket',len(basket))
            for item in basket[i][0]:
                tempDict[item] += 1
                # print('000',tempDict[item])
        for item in tempDict:
            # print('item',item)
            # print('---',tempDict[item])
            if (tempDict[item] >= perPartS):
                frequent[item] = 1
        if(len(frequent)==0):
            break
        frequentList[point] = set(frequent)
        # print('point!!!', len(frequentList[point]))
        point += 1
    for item in frequentList:
        temp = []
        temp.append(item)
        temp.append(frequentList[item])
        result.append(temp)
    return result

def phase2(iterator,basket):
    # print('baske',len(basket))
    result = []
    # for item in iterator:
    #     print('========iterator',item)
    if(len(iterator) != 0):
        if (type(iterator[0]) != str):
            result.append(len(iterator[0]))
        else:
            result.append(1)
    # print('len-r',len(result))
    # print(list(iterator))
    tempDict = collections.defaultdict(lambda: 0)
    if (len(iterator) != 0):
        for item in basket:
            for item2 in iterator:
                if(type(item2) == tuple):
                    if (all(judgePhase2(s,item) for s in item2)):
                        tempDict[item2] += 1
                else:
                    if(item2 in item):
                        tempDict[item2] += 1
    frequentList = []
    for item in tempDict:
        if (tempDict[item] >= support):
            frequentList.append(item)
    # print('-------------',frequentList)
    result.append(frequentList)
    # for item in result:
    #     print ('dddddd',item)
    return result
    # return frequentList


def judgePhase2(s,item):
    if(s in item):
        return True
    else:
        return False

# input_path = "/Users/xuxinyi/Documents/INF553/hw2/task2_dataset.csv"
# output_path = "./output.txt"
# k = 70
# support = 50
numPart = 2
k = int(sys.argv[1])
support = int(sys.argv[2])
input_path = sys.argv[3]
output_path = sys.argv[4]
perPartS = math.floor(support/numPart)


sc = SparkContext()
dataRDD = sc.textFile(input_path, numPart)
header = dataRDD.first()
# basket = dataRDD.filter(lambda x: x != header).map(lambda x: x.split(","))
basket = dataRDD.coalesce(numPart).filter(lambda x: x != header)\
    .map(lambda x: tuple(x.split(',')))\
    .groupByKey() \
    .filter(lambda x: len(set(x[1])) > k).map(lambda x: (sorted(x[1])))
bas = basket.collect()
temp = basket.mapPartitions(phase1).persist()#.map(lambda x: sorted(set(x))).persist()
# print(temp.collect())
candidate = temp.reduceByKey(lambda a,b:a|b).map(lambda x:(x[0],sorted(set(x[1])))).sortBy(lambda x:x[0]).map(lambda x:x[1]).collect()
# print(candidate)
frequentSet = temp.map(lambda x: list(x[1])).map(lambda x: phase2(x, bas)).filter(lambda x:len(x[1]) != 0).reduceByKey(lambda a,b:a+b).map(lambda x:(x[0],sorted(set(x[1])))).sortBy(lambda x:x[0]).map(lambda x: x[1]).collect()#.map(lambda x: x[1]).collect()#.reduceByKey(lambda a,b:a+b)#.foreach(lambda x:print(x))#.map(lambda x: sorted(set(x[1])))
# print(frequentSet)


with open(output_path, 'w') as f:
    f.write('Candidates:\n')
    for i in range(0, len(candidate[0])-1):
        f.write('(\''+str(candidate[0][i])+'\'),')
    f.write('(\''+str(candidate[0][len(candidate[0])-1])+'\')')
    f.write('\n')
    f.write('\n')
    for i in range(1,len(candidate)):
        for j in range(0, len(candidate[i])-1):
            f.write(str(candidate[i][j])+',')
        f.write(str(candidate[i][len(candidate[i]) - 1]))
        f.write('\n')
        f.write('\n')
    f.write('Frequent Itemsets:\n')
    for i in range(0, len(frequentSet[0])-1):
        f.write('(\''+str(frequentSet[0][i])+'\'),')
    f.write('(\''+str(frequentSet[0][len(frequentSet[0])-1])+'\')')
    f.write('\n')
    f.write('\n')
    for i in range(1,len(frequentSet)):
        for j in range(0, len(frequentSet[i])-1):
            f.write(str(frequentSet[i][j])+',')
        f.write(str(frequentSet[i][len(frequentSet[i]) - 1]))
        f.write('\n')
        f.write('\n')

time_end = time.time()
print('Duration', time_end-time_start)
