from pyspark import SparkContext, SparkConf
import argparse

#-----------CREATE SPARK CONTEXT---------------

conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("OFF")

#-----------PARSER FOR COMMAND LINE-----------

parser = argparse.ArgumentParser(description = "Classification using Naive Bayes.")

parser.add_argument('-training', '-t', dest = 'inputData', default = './data/geotweets.tsv')
parser.add_argument('-input', '-i', dest = 'inputTweet', default = './data/input1.txt')
parser.add_argument('-output', '-o', dest = 'outputFile', default = './outputFile.tsv')

args = parser.parse_args()


#----------------SUPPORT FUNCTIONS---------------

# (place, count)
def countTweetPlace(inputData):
    placeRdd = inputData.countByKey().items()
    return sc.parallelize(placeRdd)

# (place, wordListCount) - (place, [Tcw1, Tcw2,...])
def countTweetWords(inputData):
    rdd = inputData.aggregateByKey(([0]*inputCount), lambda x,y: countWords(x,y), lambda x,y: ([x[i] + y[i] for i,j in enumerate(x)]))
    return rdd

def countWords(x, words):
    #x = [0]*inputCount
    for i in range(inputCount):
        if inputTweet[i] in words:
            x[i]+=1
    return x

#----------NAIVE BAYES FOR CALCULATING PROBABILITIES--------

def calculatePropability(placesData, tweet, place):
    placeInfo = placesData.filter(lambda x: x[0] == place).collect()
    tc = placeInfo[0][1][0]
    tcwMult = reduce(lambda x,y: float(x)*float(y), placeInfo[0][1][1])
    if tc!=0:
        prob = (float(tc)/float(tweetCount))*(tcwMult/float(tc)**inputCount)
        return prob
    else:
        return 0.0

def findProbabilities(inputData, tweet):
    placesData = createRdd(inputData) #(place, (Tc, [Tcw1, Tcw2,...]))
    placesList = placesData.map(lambda x: x[0]).collect()
    if len(placesList)==0:
        return ""
    probabilities = []
    print("Number of places: ", len(placesList))
    for place in placesList:
        prob = calculatePropability(placesData, tweet, place)
        probabilities.append((place, prob))
    return probabilities

def findMaxProbability(probabilities):
    if probabilities == "":
        return ""
    probabilitiesRdd = sc.parallelize(probabilities)
    maxProbability = probabilitiesRdd.max(key=lambda x:x[1])[1]
    maxProbabilities = probabilitiesRdd.filter(lambda x: x[1]==maxProbability)
    print(maxProbabilities.take(5))
    return maxProbabilities


#---------------WRITING TO FILE-----------------

def writeToFile(maxProbabilities, outputFilename):
    outputFile = open(outputFilename, 'w')
    if maxProbabilities != "":
        if maxProbabilities.count()>1:
            toWrite = maxProbabilities
        else:
            toWrite = maxProbabilities.map(lambda x: str(x[0]) + "\t" + str(x[1]))
        outputFile.write(toWrite)
    else:
        outputFile.write("")
    outputFile.close()


#-------------CREATING INITIAL RDDs-----------
def getRdd(inputFile):
    originalRdd = sc.textFile(inputFile, use_unicode = True)\
    .map(lambda line: line.split('\t'))\
    .map(lambda x: (x[4], x[10].lower().split(" "))).sample(False, 0.1, 5)
    tweetCount = originalRdd.count()
    return originalRdd, tweetCount

def getTweetRdd(tweetFile):
    inputTweet = sc.textFile(tweetFile, use_unicode = True)\
    .map(lambda x: x.lower().split(" ")).collect()
    print("Tweet: ", inputTweet)
    inputCount = len(inputTweet)
    return inputTweet, inputCount

def createRdd(inputData):
    placeCountRdd = countTweetPlace(inputData)
    placeWordsRdd = countTweetWords(inputData)
    placeRdd = placeCountRdd.join(placeWordsRdd)
    placeRddFiltered = placeRdd.filter(lambda x: reduce(lambda i,j: i*j, x[1][1])!=0)
    return placeRddFiltered


#------------------RUN--------------------

tweet, inputCount = getTweetRdd(args.inputTweet)
originalRdd, tweetCount = getRdd(args.inputData)

maxProbabilities = findMaxProbability(findProbabilities(originalRdd, args.inputTweet))
#writeToFile(maxProbabilities, args.outputFile)
