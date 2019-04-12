from pyspark import SparkContext
import json
import time
import sys
import itertools
import random
import collections
time_start = time.time()
def generateHash(num):
    randNum = []
    while(num > 0):
        randN = random.randint(0, maxuserID)
        while randN in randNum:
            randN = random.randint(0, maxuserID)
        num -= 1
        randNum.append(randN)
    return randNum

def calMinHash(a,b):
    # print('+++++++++=')
    # # print(a)
    # print(b)
    signature = []
    for i in range(0, numhash):
        hashrow = p+1
        for item in b:
            temp =  (A[i] * item + B[i])%maxuserID
            if temp < hashrow:
                hashrow = temp
        signature.append(hashrow)
    # print(signature)
    return (a,signature)

def lshband(p):
    bus, user = p
    eachband = [user[i:i + int(len(user)/bandNum)] for i in range(0, len(user), int(len(user)/bandNum))]
    # print("+++++++++++++")
    # print(bus)
    # print(user)
    # band = [ ((i, hash(b)), [bus]) for i,b in enumerate(eachband) ]
    band = []
    for i in range(len(eachband)):
        band.append(((i, tuple(eachband[i])), [bus]))
    # print(band)
    return band

# input_path = "/Users/xuxinyi/Documents/INF553/hw3/yelp_train.csv"
# output_path = "./xinyi_xu_task1.csv"
input_path = sys.argv[1]
output_path = sys.argv[2]
sc = SparkContext()
dataRDD = sc.textFile(input_path,2)
header = dataRDD.first()
dataRDD = dataRDD.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x:(x[0],x[1])).persist()
user = list(set(dataRDD.map(lambda x: x[0]).collect()))
userId = collections.defaultdict(int)
i = 0
for item in user:
    userId[item] = i
    i += 1
maxuserID = len(userId) - 1
print(maxuserID)
business = list(set(dataRDD.map(lambda x:x[1]).collect()))
busId = collections.defaultdict(int)
i = 0
for item in business:
    busId[item] = i
    i += 1
# change to user ID and business ID
p = 11273
numhash = 120
A = generateHash(numhash)
B = generateHash(numhash)
bandRow = 3
bandNum = numhash/bandRow
dataRDD2 = dataRDD.map(lambda x: (busId[x[1]], userId[x[0]])).groupByKey().persist()
# print(dataRDD2.take(10))
dictall = collections.defaultdict(list)
for item in dataRDD2.collect():
    dictall[item[0]] = list(item[1])
# print(dictall)
resultRDD = dataRDD2.map(lambda x: calMinHash(x[0],list(x[1])))\
    .flatMap(lambda x: lshband(x)).reduceByKey(lambda a,b:a+b).filter(lambda x: len(x[1]) > 1).map(lambda x: tuple(sorted(x[1]))).distinct().collect()
# print(len(resultRDD))
c = 0
resultJud = {}
busidList = list(busId.keys())
buscontList = list(busId.values())
for item in resultRDD:
    candidate = list(itertools.combinations(item, 2))
    # print("candidate",candidate)
    for item2 in candidate:
        # print(item2)
        set1 = dictall[item2[0]]
        set2 = dictall[item2[1]]
        intersection = list(set(set1).intersection(set(set2)))
        union = list(set(set1).union(set(set2)))
        simi = len(intersection)/len(union)
        # print(simi)
        if(simi >= 0.5):
            print('+++++++++')
            temp = []
            a1 = busidList[buscontList.index(item2[0])]
            a2 = busidList[buscontList.index(item2[1])]
            temp.append(a1)
            temp.append(a2)
            temp = tuple(sorted(temp))
            resultJud[temp] = simi
            # result.append(temp)
            print(item2)
            print(simi)
            c+=1
result = []
for item in resultJud:
    temp = []
    temp.append(item)
    temp.append(resultJud[item])
    result.append(temp)
result = sorted(result)
print(result)
with open(output_path, 'w') as f:
    f.write('business_id_1, business_id_2, similarity')
    for r in result:
        f.write("\n" + r[0][0] + "," + r[0][1] + "," + str(r[1]))
# for item in resultRDD:
#     if len(item) == 2:
#         print('+++')
#         set1 = dictall[item[0]]
#         set2 = dictall[item[1]]
#         print(set1)
#         print(set2)
#         intersection = list(set(set1).intersection(set(set2)))
#         union = list(set(set1).union(set(set2)))
#         simi = len(intersection)/len(union)
#         if simi >

time_end = time.time()
print('Duration', time_end-time_start)
print (len(resultJud))


