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
#originalRdd = sc.textFile(args.training, use_unicode = True)\
        .map(lambda line: line.split('\t')).map(lambda x: (x[4], x[10].lower().split(" "))).sample(False, 0.1, 5)

#rddCount = originalRdd.count()
places = originalRdd.map(lambda x: x[0]).distinct().collect()
placeCount = originalRdd.map(lambda x: (x[0], 1)).reduceByKey(lambda x,y: x+y)

orgData = [("New York, NY", "empire state building"),
           ("New York, NY", "big empire state"),
           ("New York, NY", "new york state"),
           ("London, UK", "big ben"),
           ("London, UK", "big state building")]
originalRdd = sc.parallellize(orgData)
rddCount = originalRdd.count()


def transformInputTweet():
    print("Transforming input tweet")
    # tweet = tweetText.lower().split(" ")
    # tweetTrans = sc.textFile(args.inputTweet, use_unicode = True)
    tweet = ["state"]
    tweetRdd = sc.parallelize(tweet)
    return tweetRdd


# ((place, word), count) - sorted by descending order
def countWordRDD():
    print("Creating countWRdd")
    countWRdd = originalRdd.flatMap(lambda x: ((x[0], word) for word in x[1])) \
                    .map(lambda x: ((x[0], x[1]), 1)) \
                    .reduceByKey(lambda x,y: x+y) \
                    .sortBy(lambda x: x[1], ascending = False)
    return countWRdd


def countTweetsPerPlace(place): #Tc
    tweetCount = placeCount.filter(lambda x: x[0] == place).map(lambda x: x[1])
    return tweetCount.collect()[0]

def countWordsPerPlace(countWRdd, inputWord): #Tcw
    wordsPerPlace = countWRdd.filter(lambda x: x[0][1]==inputWord).map(lambda x: x[1]).collect()
    print("Tcw: ", wordsPerPlace)
    if len(wordsPerPlace) == 0:
        return 0.0
    else:
        return float(wordsPerPlace[0])

'''def calculatePlaceWordProbability(countWRdd, place, word):
    print("Calculating place-word probability for word", word)
    Tcw = countWordsPerPlace(countWRdd, place, word)
    print("Tcw: ", Tcw)
    return Tcw'''


def calculatePlaceProbability(countWRdd, place, tweet):
    print("Calculating place probability for place", place)
    Tc = countTweetsPerPlace(place)
    print("Tc: ", Tc)
    if Tc == 0:
        return 0.0
    prob = Tc / rddCount
    placeWRdd = countWRdd.filter(lambda x: x[0][0] == place)
    for word in tweet:
        wordRdd = placeWRdd.filter(lambda x: x[0][1] == word)
        prob *= countWordsPerPlace(wordRdd, word)
    #prob2 = map(lambda word: countWordsPerPlace(placeWRdd, word)/Tc, tweet)
    #prob = sc.parallelize(prob2).reduce(lambda x,y: x*y)
    #prob2 = []
    #wordCount = inputTweetRdd.map(lambda word: calculatePlaceWordProbability(placeWRdd, place, word)/Tc)
    #for word in tweet:
    #    prob2.append(calculatePlaceWordProbability(countWRdd, place, word))
    #prob = wordCount.reduce(lambda x, y: x*y)
    return prob

def calculateProbForAllPlaces(countWRdd, inputTweetRdd):
    print("Calculating probabilities for all places")
    probabilities = []
    tweet = inputTweetRdd.collect()
    for place in places:
        probabilities.append(calculatePlaceProbability(countWRdd, place, tweet))
    #placeProb = places.map(lambda place: calculatePlaceProbability(countWRdd, place, inputTweetRdd))
    return placeProb

# x = place, y = probability
# Can be more than one outcome
def writeFile(rdd):
    # If there's one or more places found
    if rdd.count() > 0:
        rdd.map(lambda (x,y): x + "\t" + str(y)).coalesce(1).saveAsTextFile(args.outputFile)
    # If there's no place with prob greater than zero
    else:
        results = sc.parallellize([])
        results.coalesce(1).saveAsTextFile(args.outputFile)

def main():
    inputTweet = transformInputTweet()
    countWRdd = countWordRDD()
    probabilities = calculateProbForAllPlaces(countWRdd, inputTweet)
    probabilities.take(5)
    #maxProb = probabilities.max()
    #print(maxProb)

main()

#training_rdd = sc.parallelize([(("NY", "hello"), 100), (("NY", "hi"), 50), (("London", "world"), 10)])
