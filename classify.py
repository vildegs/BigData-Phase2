from pyspark import SparkContext, SparkConf
import argparse

conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("OFF")

# Creates an RDD from values #5 place_name and #11 tweet_text
parser = argparse.ArgumentParser(description = "Classification using Naive Bayes.")

parser.add_argument('-training', '-t', dest = 'training', default = './data/geotweets.tsv')
parser.add_argument('-input', '-i', dest = 'inputTweet')
parser.add_argument('-output', '-o', dest = 'outputFile', default = './outputFile.tsv')

args = parser.parse_args()
originalRdd = sc.textFile(args.training, use_unicode = True)\
        .map(lambda line: line.split('\t')).map(lambda x: (x[4], x[10].lower().split(" "))).sample(False, 0.001, 5)
rddCount = originalRdd.count()
places = originalRdd.map(lambda x: x[0]).distinct()
placeCount = originalRdd.map(lambda x: (x[0], 1)).reduceByKey(lambda x,y: x+y)
#print(placeCount.take(5))


def transformInputTweet():
    print("Transforming input tweet")
    # tweet = tweetText.lower().split(" ")
    # tweetTrans = sc.textFile(args.inputTweet, use_unicode = True)
    tweet = ["hello"]
    tweetRdd = sc.parallelize(tweet)
    return tweetRdd


# ((place, word), count) - sorted by descending order
def countWordRDD():
    print("Creating countWRdd")
    countWRdd = originalRdd.flatMap(lambda x: ((x[0], word) for word in x[1])) \
                    .map(lambda x: ((x[0], x[1]), 1)) \
                    .reduceByKey(lambda x,y: x+y) \
                    .sortBy(lambda x: x[1], ascending = False)

    print ("countWRdd", countWRdd.take(5))
    return countWRdd


def countTweetsPerPlace(place): #Tc
    #tweetCount = originalRdd.filter(lambda x: x[0] == place).count()
    tweetCount = placeCount.lookup(place)
    print ("tweetCount", tweetCount)
    return tweetCount

def countWordsPerPlace(countWRdd, place, inputWord): #Tcw
    wordsPerPlace = countWRdd.filter(lambda x: x[0][0] == place and x[0][1] == inputWord).map(lambda x: x[1])
    if wordsPerPlace.isEmpty():
        return 0
    else:
        return float(wordsPerPlace.collect()[0])

def calculatePlaceWordProbability(countWRdd, place, word):
    print("Calculating place-word probability for word", word)
    Tc = countTweetsPerPlace(place)
    print("Tc: ", Tc)
    Tcw = countWordsPerPlace(countWRdd, place, word)
    print("Tcw: ", Tcw)
    if Tc == 0 or Tcw == 0:
        return 0
    placeWordProb = Tcw / Tc
    return placeWordProb


def calculatePlaceProbability(countWRdd, place, inputTweetRdd):
    print("Calculating place probability for place", place)
    prob1 = countTweetsPerPlace(place) / rddCount
    tweet = inputTweetRdd.collect()
    prob2 = []
    for word in tweet:
        prob2.append(calculatePlaceWordProbability(countWRdd, place, word))
    prob = sc.parallelize(prob2).reduce(lambda x,y: x*y)
    print(place,": ", prob*prob1)
    return prob1*prob



def calculateProbForAllPlaces(countWRdd, inputTweetRdd):
    print("Calculating probabilities for all places")
    probabilities = []
    for place in places.collect():
        probabilities.append(calculatePlaceProbability(countWRdd, place, inputTweetRdd))
    #places.map(lambda place: calculatePlaceWordProbability(countWRdd, place, inputTweetRdd))
    return probabilities

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
    maxProb = calculateProbForAllPlaces(countWRdd, inputTweet).max()
    print(maxProb)

main()

#training_rdd = sc.parallelize([(("NY", "hello"), 100), (("NY", "hi"), 50), (("London", "world"), 10)])
