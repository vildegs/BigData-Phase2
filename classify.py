from pyspark import SparkContext, SparkConf
import argparse

#-----------CREATE SPARK CONTEXT---------------

conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("OFF")

#-----------PARSER FOR COMMAND LINE-----------

parser = argparse.ArgumentParser(description = "Classification using Naive Bayes.")

parser.add_argument('-training', '-t', dest = 'trainingData', default = './data/geotweets.tsv')
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

# Joins two rdds - one with word counts and one with tweet count per place.
# Filters out all places where at least one of the words in the input tweet doesn't occur.
def createRdd(trainingData):
    placeCountRdd = countTweetPlace(trainingData)
    placeWordsRdd = countTweetWords(trainingData)
    placeRdd = placeCountRdd.join(placeWordsRdd)
    placeRddFiltered = placeRdd.filter(lambda x: reduce(lambda i,j: i*j, x[1][1])!=0)
    return placeRddFiltered


#----------------SUPPORT FUNCTIONS---------------

# Counts number of tweets from all places using countByKey().
# Returns rdd on the form (place, count).
def countTweetPlace(trainingData):
    placeTweetCountRdd = trainingData.countByKey().items()
    return sc.parallelize(placeTweetCountRdd)

# Counts number of tweets a given word from input tweet appears in at a given place.
# Returns rdd on the form (place, [Tcw1, Tcw2,..., Tcwn]).
def countTweetWords(trainingData):
    rdd = trainingData.aggregateByKey(([0]*testingTweetWordCount), lambda x,y: countWords(x,y), lambda x,y: ([x[i] + y[i] for i,j in enumerate(x)]))
    return rdd

# Counts number of times a word from input tweet occurs in a given tweet.
def countWords(countList, words):
    # countList = [0]*testingTweetWordCount
    for i in range(testingTweetWordCount):
        if testingTweet[i] in words:
            countList[i]+=1
    return countList

#----------NAIVE BAYES FOR CALCULATING PROBABILITIES--------

# Calculates probability for a given place based on input tweet.
# Multiplies all tcw and returns probability.
def calculatePropability(placesDataRdd, tweet, place):
    placeInfo = placesDataRdd.filter(lambda x: x[0] == place).collect()
    tc = placeInfo[0][1][0]
    tcwMult = reduce(lambda x,y: float(x)*float(y), placeInfo[0][1][1])
    if tc!=0:
        probability = (float(tc)/float(trainingRddCount))*(tcwMult/(float(tc)**testingTweetWordCount))
        return probability
    else:
        return 0.0

# Calculates probabilities for all plaxces.
# Iterates through list of places and returns all probabilities.
def findProbabilities(trainingData, testingTweet):
    placesDataRdd = createRdd(trainingData) #(place, (Tc, [Tcw1, Tcw2,...]))
    placesList = placesDataRdd.map(lambda x: x[0]).collect()
    if len(placesList)==0:
        return ""
    probabilities = []
    print("Number of places: ", len(placesList))
    i = 0
    for place in placesList:
        if i%100 == 0:
            print("Iteration", i)
        placeProbability = calculatePropability(placesDataRdd, testingTweet, place)
        probabilities.append((place, placeProbability))
        i+=1
    return probabilities

# Uses findProbabilities to find the max probabilities (all occurances).
# Returns rdd with max probabilities with corresponding place.
def findMaxProbability(probabilities):
    if probabilities == "":
        return ""
    probabilitiesRdd = sc.parallelize(probabilities)
    maxProbability = probabilitiesRdd.max(key=lambda x:x[1])[1]
    maxProbabilities = probabilitiesRdd.filter(lambda x: x[1]==maxProbability)
    return maxProbabilities


#---------------WRITING TO FILE-----------------

# Writes to file.
# Writes all max probabilities with places, or just "" if no place has probabilitiy greater than 0.
def writeToFile(maxProbabilities, outputFilename):
    print("Writing to file")
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
trainingRdd, trainingRddCount = getTrainingRdd(args.trainingData)

# Calculating all probabilities based on trainingRdd and testingTweet
# Finds max probability
# Writes all probabilities to file
probabilities = findProbabilities(trainingRdd, testingTweet)
maxProbabilities = findMaxProbability(probabilities)
writeToFile(maxProbabilities, args.outputFile)
