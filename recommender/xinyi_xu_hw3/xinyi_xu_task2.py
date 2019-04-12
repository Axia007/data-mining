from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkContext
import collections
import time
import math
import itertools
import random
import sys
time_start = time.time()

def calW3(x):
    user = x[0]
    business = x[1]
    itemPer = bustouser[business]
    canItem = usertobus[user]
    # userPer = usertobus[user]
    # canUser = bustouser[business]
    weightall = {}
    # avg = {}
    for item in canItem:
        aaa = []
        aaa.append(business)
        aaa.append(item)
        if(tuple(sorted(aaa)) not in resultJud):
            continue
        businessu = bustouser[item].keys()
        corateduser = list(set(businessu).intersection(set(itemPer)))
        if(len(corateduser) > 0):
            sum1 = 0
            sum2 = 0
            for item2 in corateduser:
                sum1 += bustouser[business][item2]
                sum2 += bustouser[item][item2]
            activeavg = sum1/len(corateduser)
            otheravg = sum2/len(corateduser)
            sumup = 0
            sumdown1 = 0
            sumdown2 = 0
            for item2 in corateduser:
                sumup += (bustouser[business][item2]- activeavg) * (bustouser[item][item2] - otheravg)
                sumdown1 += pow(bustouser[business][item2]- activeavg,2)
                sumdown2 += pow(bustouser[item][item2] - otheravg,2)
            a = math.sqrt(sumdown1) * math.sqrt(sumdown2)
            if(a==0):
                continue
            weight = sumup/a
            # if(weight < 0):
            #     continue
            weightall[item] = weight
    sum3 = 0
    sum4 =0
    weightall = sorted(weightall.items(), key=lambda x: x[1], reverse=True)
    k = 0
    pearson = average[business]
    if(k > 5):
        for item in weightall:
            if(k > 50):
                break

            sum3 += (bustouser[item][user] - average[item]) * weightall[item]
            sum4 += abs(weightall[item])
            k+=1
        if (sum4 != 0):
            pearson = sum3/sum4
        # if(pearson >15):
        #     pearson =5
        if(pearson > 5 ):
            pearson = 4.7
        if(pearson <0):
            pearson = 0.5
    # print(pearson)
    return ((user,business), pearson)

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


def maptodic(x):
    dicnow = {}
    for item in list(x[1]):
        dicnow[item[0]] = item[1]
    return (x[0],dicnow)

def calW1(x):
    user = x[0]
    business = x[1]
    itemPer = bustouser[business]
    canItem = usertobus[user]
    # userPer = usertobus[user]
    # canUser = bustouser[business]
    weightall = {}
    # avg = {}
    for item in canItem:
        businessu = bustouser[item].keys()
        corateduser = list(set(businessu).intersection(set(itemPer)))
        if(len(corateduser) > 0):
            sum1 = 0
            sum2 = 0
            for item2 in corateduser:
                sum1 += bustouser[business][item2]
                sum2 += bustouser[item][item2]
            activeavg = sum1/len(corateduser)
            otheravg = sum2/len(corateduser)
            sumup = 0
            sumdown1 = 0
            sumdown2 = 0
            for item2 in corateduser:
                sumup += (bustouser[business][item2]- activeavg) * (bustouser[item][item2] - otheravg)
                sumdown1 += pow(bustouser[business][item2]- activeavg,2)
                sumdown2 += pow(bustouser[item][item2] - otheravg,2)
            a = math.sqrt(sumdown1) * math.sqrt(sumdown2)
            if(a==0):
                continue
            weight = sumup/a
            # if(weight < 0):
            #     continue
            weightall[item] = weight
    sum3 = 0
    sum4 =0
    weightall = sorted(weightall.items(), key=lambda x: x[1], reverse=True)
    k = 0
    pearson = average[business]
    if(k > 5):
        for item in weightall:
            if(k > 50):
                break

            sum3 += (bustouser[item][user] - average[item]) * weightall[item]
            sum4 += abs(weightall[item])
            k+=1
        if (sum4 != 0):
            pearson = sum3/sum4
        # if(pearson >15):
        #     pearson =5
        if(pearson > 5 ):
            pearson = 4.7
        if(pearson <0):
            pearson = 0.5
    # print(pearson)
    return ((user,business), pearson)

def calW(x):
    user = x[0]
    business = x[1]
    userPer = usertobus[user]
    canUser = bustouser[business]
    weightall = {}
    avg = {}
    for item in canUser:
        usersb = usertobus[item].keys()
        # if(business not in usersb):
        #     print('++++++++++++++++++++++++++++++++++++++++')
        # if (business not in userPer):
        #     print('=======================================')
        corateditem = list(set(usersb).intersection(set(userPer)))
        if(len(corateditem) > 0):
            sum1 = 0
            sum2 = 0
            for item2 in corateditem:
                sum1 += usertobus[user][item2]
                sum2 += usertobus[item][item2]
            activeavg = sum1/len(corateditem)
            otheravg = sum2/len(corateditem)
            sumup = 0
            sumdown1 = 0
            sumdown2 = 0
            for item2 in corateditem:
                sumup += (usertobus[user][item2]- activeavg) * (usertobus[item][item2] - otheravg)
                sumdown1 += pow(usertobus[user][item2]- activeavg,2)
                sumdown2 += pow(usertobus[item][item2] - otheravg,2)
            a = math.sqrt(sumdown1) * math.sqrt(sumdown2)
            if(a==0):
                continue
            weight = sumup/a
            # if(weight < 0):
            #     continue
            weightall[item] = weight
    sum3 = 0
    sum4 =0
    weightall = sorted(weightall.items(), key=lambda x: x[1], reverse=True)
    # print(weightall)
    k = 0
    pearson = average[user]
    if(k > 5):
        for item in weightall:
            if(k > 50):
                break

            sum3 += (usertobus[item][business] - average[item]) * weightall[item]
            sum4 += abs(weightall[item])
            k+=1
        if (sum4 != 0):
            pearson += sum3/sum4
        # if(pearson >15):
        #     pearson =5
        # if(pearson > 5 ):
        #     pearson = 5
        # if(pearson <0):
        #     pearson = 0.5
    # print(pearson)
    return ((user,business), pearson)


        # print('===',corateditem)
    # print("user", userPer)
    # print('bus', canUser)

# Load and parse the data
# train_file_name = "/Users/xuxinyi/Documents/INF553/hw3/yelp_train.csv"
# test_file_name = "/Users/xuxinyi/Documents/INF553/hw3/yelp_val.csv"
# output_path = "./xinyi_xu_task1.csv"
# case = 4
train_file_name = sys.argv[1]
test_file_name = sys.argv[2]
case = int(sys.argv[3])
output_path = sys.argv[4]
sc = SparkContext()
dataRDD = sc.textFile(train_file_name)
header = dataRDD.first()
dataRDD = dataRDD.filter(lambda x: x != header).map(lambda x: x.split(',')).persist()
user = list(set(dataRDD.map(lambda x: x[0]).collect()))
userId = collections.defaultdict(int)
reuserId = collections.defaultdict(str)
i = 0
for item in user:
    userId[item] = i
    reuserId[i] = item
    i += 1
maxuserID = len(userId) - 1
# print(maxuserID)
business = list(set(dataRDD.map(lambda x:x[1]).collect()))
busId = collections.defaultdict(int)
reubusId = collections.defaultdict(str)
j = 0
for item in business:
    busId[item] = j
    reubusId[j] = item
    j += 1
dataRDD2 = dataRDD.map(lambda x: (int(userId[x[0]]), int(busId[x[1]]),float(x[2]))).persist()
dataRDD.unpersist()




testRDD = sc.textFile(test_file_name).filter(lambda x: x != header).map(lambda x: x.split(',')).persist()
newitemRDD = testRDD.filter(lambda x: (x[1] not in busId) and (x[0] in userId)).persist()
newuserRDD = testRDD.filter(lambda x: (x[0] not in userId) and (x[1] in busId)).persist()
allRDD = testRDD.filter(lambda x: (x[0] not in userId) and (x[1] not in busId)).persist()
user2 = list(set(testRDD.map(lambda x: x[0]).collect()))
for item in user2:
    if item not in userId:
        userId[item] = i
        reuserId[i] = item
        i += 1
business2 = list(set(testRDD.map(lambda x:x[1]).collect()))
for item in business2:
    if item not in busId:
        busId[item] = j
        reubusId[j] = item
        j += 1

if case == 4:
    # change to user ID and business ID
    p = 11273
    numhash = 120
    A = generateHash(numhash)
    B = generateHash(numhash)
    bandRow = 3
    bandNum = numhash / bandRow
    dataRDD5 = dataRDD.map(lambda x: (busId[x[1]], userId[x[0]])).groupByKey().persist()
    # print(dataRDD2.take(10))
    dictall = collections.defaultdict(list)
    for item in dataRDD5.collect():
        dictall[item[0]] = list(item[1])
    # print(dictall)
    resultRDD = dataRDD5.map(lambda x: calMinHash(x[0], list(x[1]))) \
        .flatMap(lambda x: lshband(x)).reduceByKey(lambda a, b: a + b).filter(lambda x: len(x[1]) > 1).map(
        lambda x: tuple(sorted(x[1]))).distinct().collect()
    # print(len(resultRDD))
    # c = 0
    resultJud = {}
    # busidList = list(busId.keys())
    # buscontList = list(busId.values())
    for item in resultRDD:
        candidate = list(itertools.combinations(item, 2))
        # print("candidate",candidate)
        for item2 in candidate:
            # print(item2)
            set1 = dictall[item2[0]]
            set2 = dictall[item2[1]]
            intersection = list(set(set1).intersection(set(set2)))
            union = list(set(set1).union(set(set2)))
            simi = len(intersection) / len(union)
            # print(simi)
            if (simi >= 0.5):
                print('+++++++++')
                temp = []
                # a1 = busidList[buscontList.index (item2[0])]
                # a2 = busidList[buscontList.index(item2[1])]
                temp.append(item2[0])
                temp.append(item2[1])
                temp = tuple(sorted(temp))
                resultJud[temp] = simi

# new item
newitemRDD2 = newitemRDD.map(lambda x: (int(userId[x[0]]), int(busId[x[1]])))
newitemRDD.unpersist()
newitemu = newitemRDD2.map(lambda x:x[0]).distinct().collect()
trainUser= dataRDD2.filter(lambda x: x[0] in newitemu).map(lambda x:(x[0],x[2]))\
    .aggregateByKey((0,0),lambda a,b:(a[0]+b,a[1]+1), lambda a1,a2: (a1[0]+a2[0],a1[1]+a2[1])).map(lambda x: (x[0], float(x[1][0])/x[1][1])).collect()
userRate1 = collections.defaultdict(float)
for item in trainUser:
    # print(item)
    userRate1[item[0]] = item[1]
newitem3 = newitemRDD2.map(lambda x: (x[0], x[1], userRate1[x[0]]))
# ratings5 = ratings.union(newitem3)

# new user
print('================',len(newuserRDD.collect()))
newuserRDD2 = newuserRDD.map(lambda x: (int(userId[x[0]]), int(busId[x[1]])))
newuserRDD.unpersist()
newuseri = newuserRDD2.map(lambda x:x[1]).distinct().collect()
trainItem= dataRDD2.filter(lambda x: x[1] in newuseri).map(lambda x:(x[1],x[2]))\
    .aggregateByKey((0,0),lambda a,b:(a[0]+b,a[1]+1), lambda a1,a2: (a1[0]+a2[0],a1[1]+a2[1])).map(lambda x: (x[0], float(x[1][0])/x[1][1])).collect()
itemRate1 = collections.defaultdict(float)
for item in trainItem:
    # print(item)
    itemRate1[item[0]] = item[1]
newuser3 = newuserRDD2.map(lambda x: (x[0], x[1], itemRate1[x[1]]))

# # all new
newallRDD= allRDD.map(lambda x: (int(userId[x[0]]), int(busId[x[1]]), 3.0))
allRDD.unpersist()
# newallRDD2 = newallRDD.map(lambda x: (x[0], x[1], x[]))

# ratings = ratings.union(newallRDD2)
trainRDDall = dataRDD2.union(newitem3).union(newallRDD).union(newuser3)
dataRDD2.unpersist()
# print(trainRDDall.take(10))
testRDD2 = testRDD.map(lambda x: (int(userId[x[0]]), int(busId[x[1]]),float(x[2])))
testRDD.unpersist()
usertobus = trainRDDall.map(lambda x: (x[0],(x[1],x[2]))).groupByKey().map(lambda x: maptodic(x)).collectAsMap()
bustouser = trainRDDall.map(lambda x: (x[1],(x[0],x[2]))).groupByKey().map(lambda x: maptodic(x)).collectAsMap()
average = {}
# testRDD3 = testRDD2
if case == 1:
    ratings = trainRDDall.map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))
    rank = 2
    numIterations = 10
    model = ALS.train(ratings, rank, numIterations, 0.2)
    testRDD2 = testRDD.map(lambda x: (int(userId[x[0]]), int(busId[x[1]]), float(x[2])))
    test = testRDD2.map(lambda x: Rating(x[0], x[1], x[2])).map(lambda x:(x[0],x[1]))
    testRDD3 = model.predictAll(test).map(lambda r: ((r[0], r[1]), r[2]))
elif case == 2:
    for item in usertobus:
        av = usertobus[item].values()
        average[item] = sum(av)/len(av)
    testRDD3 = testRDD2.map(lambda x:calW(x))
elif case == 3:
    for item in bustouser:
        av = bustouser[item].values()
        average[item] = sum(av) / len(av)
    testRDD3 = testRDD2.map(lambda x: calW1(x))
elif case == 4:
    for item in bustouser:
        av = bustouser[item].values()
        average[item] = sum(av) / len(av)
    testRDD3 = testRDD2.map(lambda x: calW3(x))
ratesAndPreds = testRDD2.map(lambda r: ((r[0], r[1]), r[2])).join(testRDD3)
print(len(ratesAndPreds.collect()))
# dddd= ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])).foreach(lambda x:print(x))
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))

result = testRDD3.map(lambda x : (x[0][0],x[0][1],x[1])).map(lambda x: (reubusId[x[0]],reubusId[x[1]], x[2])).collect()
time_end = time.time()
print('Duration', time_end-time_start)

with open(output_path, 'w') as f:
    f.write('user_id, business_id, prediction')

    # for item in result:
    #     a1 = useridList[usercontList.index(item[0])]
    #     a2 = busidList[buscontList.index(item[1])]
    #     f.write(a1+',')
    #     f.write(a2+',')
    #     f.write(str(item[2]))
    #     f.write('\n')
    for r in result:
        f.write("\n" + r[0] + "," + r[1] + "," + str(r[2]))


time_end = time.time()
print('Duration', time_end-time_start)

