from pyspark import SparkContext
import json
import time
import sys
import math
import collections
import itertools
import numpy as np

    # input_path =
    # output_path = sys.argv[2]
    # time_start = time.time()

# input_path = "/Users/xuxinyi/Documents/INF553/hw2/small2.csv"
# output_path = "./xinyi_xu_task1.txt"
caseNum = int(sys.argv[1])
support = int(sys.argv[2])
input_path = sys.argv[3]
output_path = sys.argv[4]
numPart = 2
perPartS = math.floor(support/numPart)

def countf(iterator):
    basket = iterator
    tempDict = collections.defaultdict(lambda: 0)
    frequentList = collections.defaultdict(list)
    basket = []
    for item in iterator:
        a = item
        basket.append(a)
        for item2 in item:
            tempDict[item2] += 1
    for item in tempDict:
        if (tempDict[item] >= perPartS):
            frequentList[1].append(item)
    flag = 2
    frequentList[1] = sorted(frequentList[1])
    singleList = frequentList[1]
    # print('basket',basket)


    while(flag <= len(singleList)):
        # print(flag)
        mayFrequet = [list(itertools.combinations(item, flag)) for item in basket]
        tempDict = collections.defaultdict(lambda: 0)
        # count
        for item in mayFrequet:
            for item2 in item:
                tempDict[item2] += 1
        for item in tempDict:
            if (tempDict[item] >= perPartS):
                frequentList[flag].append(item)
        # print(frequentList)
        singleList = []
        for item in frequentList[flag]:
            for item2 in item:
                singleList.append(item2)
        singleList = list(set(singleList))
        # print('singleList',singleList)
        tempBasket = []
        for i in range(0, len(basket)):
            temp = []
            for j in range(0, len(basket[i])):
                if (basket[i][j] in singleList):
                    temp.append(basket[i][j])
            tempBasket.append(temp)
        basket = tempBasket
        # print('basket',basket)

        flag += 1

    result = []
    for item in frequentList:
        if(len(frequentList[item]) != 0):
            temp = []
            temp.append(item)
            temp.append(frequentList[item])
            result.append(temp)
    # for item in result:
    #     print ('dddddd',item)
    return result
    # return frequentList

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



time_start = time.time()
sc = SparkContext()
dataRDD = sc.textFile(input_path, numPart)
header = dataRDD.first()
# basket = dataRDD.filter(lambda x: x != header).map(lambda x: x.split(","))
if(caseNum == 1):
    basket = dataRDD.filter(lambda x: x != header).map(lambda x: x.split(",")).map(lambda x: (x[0], x[1])) \
        .groupByKey().map(lambda x: (sorted(set(x[1]))))
elif(caseNum == 2):
    basket = dataRDD.filter(lambda x: x != header).map(lambda x: x.split(",")).map(lambda x: (x[1], x[0])) \
        .groupByKey().map(lambda x: (sorted(set(x[1]))))
bas = basket.collect()
temp = basket.mapPartitions(countf)#.foreach(lambda x:print(x))#.map(lambda x: sorted(set(x))).persist()
# a = temp.foreach(lambda x:print(x))
candidate = temp.reduceByKey(lambda a,b:a+b).map(lambda x:(x[0],sorted(set(x[1])))).sortBy(lambda x:x[0]).map(lambda x: x[1]).collect()
# print(candidate)
frequentSet = temp.map(lambda x: x[1]).map(lambda x: phase2(x, bas)).filter(lambda x:len(x[1]) != 0).reduceByKey(lambda a,b:a+b).map(lambda x:(x[0],sorted(set(x[1])))).sortBy(lambda x:x[0]).map(lambda x: x[1]).collect()#.map(lambda x: x[1]).collect()#.reduceByKey(lambda a,b:a+b)#.foreach(lambda x:print(x))#.map(lambda x: sorted(set(x[1])))
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
