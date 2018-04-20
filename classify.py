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
parser.add_argument('-output', '-o', dest = 'outputFile', default = './data/output1.tsv')

args = parser.parse_args()

#-------------CREATING INITIAL RDDs-----------

# Transforms training set from textfile to rdd
# Returns training set as rdd along with number of elements in the data set
def getTrainingRdd(inputTestingFile):
    trainingRdd = sc.textFile(inputTestingFile, use_unicode = True)\
    .map(lambda line: line.split('\t'))\
    .map(lambda x: (x[4], x[10].lower().split(" "))).sample(False, 0.1, 5)
    trainingRddCount = trainingRdd.count()
    return trainingRdd, trainingRddCount

# Transforms input tweet from textfile to list.
# Returns testing tweet as a list along with number of words in the tweet.
def getTestingTweet(testingTweetFile):
    testingTweet = sc.textFile(testingTweetFile, use_unicode = True)\
    .map(lambda x: x.lower().split(" ")).collect()
    print("Tweet text: ", testingTweet[0])
    testingTweetWordCount = len(testingTweet)
    return testingTweet[0], testingTweetWordCount

#
def createRdd(inputData):
    placeCountRdd = countTweetPlace(inputData)
    placeWordsRdd = countTweetWords(inputData)
    placeRdd = placeCountRdd.join(placeWordsRdd)
    placeRddFiltered = placeRdd.filter(lambda x: reduce(lambda i,j: i*j, x[1][1])!=0)
    return placeRddFiltered


#----------------SUPPORT FUNCTIONS---------------

# Counts number of tweets from all places using countByKey()
# Returns rdd on the form (place, count)
def countTweetPlace(inputData):
    placeTweetCountRdd = inputData.countByKey().items()
    return sc.parallelize(placeTweetCountRdd)

# Counts number of tweets a given word from input tweet appears in at a given place
# Returns rdd on the form (place, [Tcw1, Tcw2,..., Tcwn])
def countTweetWords(inputData):
    rdd = inputData.aggregateByKey(([0]*testingTweetWordCount), lambda x,y: countWords(x,y), lambda x,y: ([x[i] + y[i] for i,j in enumerate(x)]))
    return rdd

def countWords(x, words):
    #x = [0]*testingTweetWordCount
    for i in range(testingTweetWordCount):
        if tweet[i] in words:
            x[i]+=1
    return x

#----------NAIVE BAYES FOR CALCULATING PROBABILITIES--------

def calculatePropability(placesData, tweet, place):
    placeInfo = placesData.filter(lambda x: x[0] == place).collect()
    tc = placeInfo[0][1][0]
    tcwMult = reduce(lambda x,y: float(x)*float(y), placeInfo[0][1][1])
    if tc!=0:
        prob = (float(tc)/float(trainingRddCount))*(tcwMult/float(tc)**testingTweetWordCount)
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
    i = 0
    for place in placesList:
        if i%100 == 0:
            print("Iteration", i)
        prob = calculatePropability(placesData, tweet, place)
        probabilities.append((place, prob))
        i+=1
    return probabilities

def findMaxProbability(probabilities):
    if probabilities == "":
        return ""
    probabilitiesRdd = sc.parallelize(probabilities)
    maxProbability = probabilitiesRdd.max(key=lambda x:x[1])[1]
    maxProbabilities = probabilitiesRdd.filter(lambda x: x[1]==maxProbability)
    return maxProbabilities


#---------------WRITING TO FILE-----------------

def writeToFile(maxProbabilities, outputFilename):
    print("Skriver til fil")
    outputFile = open(outputFilename, 'w')
    if maxProbabilities != "":
        if maxProbabilities.count()>1:
            places = maxProbabilities.map(lambda x: x[0]).collect()
            maxProb = maxProbabilities.map(lambda x: x[1]).collect()[0]
            for place in places:
                outputFile.write(place + "\t")
            outputFile.write(str(maxProb))
        else:
            maxProbabilitiesList = maxProbabilities.collect()
            outputFile.write(str(maxProbabilitiesList[0][0]) + "\t")
            outputFile.write(str(maxProbabilitiesList[0][1]))
    else:
        outputFile.write("")
    outputFile.close()


#------------------RUN AND INITIALIZATION--------------------

# Creating rdds for test tweet and training set
# Counting number of words and elementd in respective rdds
testingTweet, testingTweetWordCount = getTestingTweet(args.inputTweet)
trainingRdd, trainingRddCount = getTrainingRdd(args.inputData)

# Calculating all probabilities based on trainingRdd and testingTweet
#Finds max probability
#Writes all probabilities to file
probabilities = findProbabilities(trainingRdd, testingTweet)
maxProbabilities = findMaxProbability(probabilities)
writeToFile(maxProbabilities, args.outputFile)
