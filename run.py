from pyspark import SparkContext, SparkConf
import argparse

conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("OFF")

parser = argparse.ArgumentParser(description = "Classification using Naive Bayes.")

parser.add_argument('-training', '-t', dest = 'training', default = './data/geotweets.tsv')
parser.add_argument('-input', '-i', dest = 'inputTweet')
parser.add_argument('-output', '-o', dest = 'outputFile', default = './outputFile.tsv')

args = parser.parse_args()
originalRdd = sc.textFile(args.training, use_unicode = True).map(lambda line: line.split('\t')).map(lambda x: (x[4], x[10].lower().split(" "))).sample(False, 1, 5)
tweetCount = originalRdd.count()

'''orgData = [("New York, NY", "empire state building"),
           ("New York, NY", "big empire state"),
           ("New York, NY", "new york state"),
           ("London, UK", "state"),
           ("London, UK", "big state building"),
           ("London, UK", "big ben state"),]

rdd = sc.parallelize(orgData)
originalRdd = rdd.map(lambda x: (x[0], x[1].lower().split(" ")))
tweetCount = originalRdd.count()'''

inputTweet = ["great", "job"]
inputCount = len(inputTweet)

def tweetsPerPlace(place): #Tc - sett inn .count()
    tweetCount = placeCount.filter(lambda x: x[0] == place).map(lambda x: x[1])
    return tweetCount.collect()[0]

def createRdd(inputData):
    placeCountRdd = countTweetPlace(inputData)
    placeWordsRdd = countTweetWords(inputData)
    #placeWordsRdd = mergeTweets(inputData)
    placeRdd = placeCountRdd.join(placeWordsRdd)
    #print(placeRdd.take(5))
    #places = placeRdd.map(lambda x: (x[0], (x[1][0], countWords(x[1][1]))))
    placeRddFiltered = placeRdd.filter(lambda x: reduce(lambda i,j: i*j, x[1][1])!=0)
    # lambda x: sum(x[1][1])!=0
    return placeRddFiltered


def countWords(x, words):
    #x = [0]*inputCount
    for i in range(inputCount):
        if inputTweet[i] in words:
            x[i]+=1
    return x

# (place, count)
def countTweetPlace(inputData):
    placeRdd = inputData.countByKey().items()
    return sc.parallelize(placeRdd)

# (place, wordListCount)
def countTweetWords(inputData):
    rdd = inputData.aggregateByKey(([0]*inputCount), lambda x,y: countWords(x,y), lambda x,y: ([x[i] + y[i] for i,j in enumerate(x)]))
    return rdd

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
        #print("Place: ", place, "Prob: ", prob)
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

def writeToFile(maxProbabilities):
    outputFile = open(args.output, 'w')
    if maxProbabilities != "":
        if maxProbabilities.count()>1:
            toWrite = maxProbabilities
        else:
            toWrite = maxProbabilities.map(lambda x: str(x[0]) + "\t" + str(x[1]))
        outputFile.write(toWrite)
    else:
        outputFile.write("")
    outputFile.close()



def main():
    #createRdd(originalRdd, "")
    #countTweetWords(originalRdd)
    #calculatePropability(originalRdd, inputTweet, "London, UK")
    findMaxProbability(findProbabilities(originalRdd, inputTweet))

main()
