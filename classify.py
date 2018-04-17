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
rdd = sc.textFile(args.training, use_unicode = True)\
        .map(lambda line: line.split('\t')).map(lambda x: (x[4], x[10].lower().split(" ")))
rdd_sample = rdd.sample(False, 0.1, 5)
rddCount = rdd_sample.count()

# ((place, word), count) - sorted by descending order
def countRDD(rdd):
    rddWordCount = rdd.flatMap(lambda x: ((x[0], word) for word in x[1])) \
                    .map(lambda x: ((x[0], x[1]), 1)) \
                    .reduceByKey(lambda x,y: x+y) \
                    .sortBy(lambda x: x[1], ascending = False)
    return rddWordCount


def transformInputTweet():
    # tweet = tweetText.lower().split(" ")
    # tweetTrans = sc.textFile(args.inputTweet, use_unicode = True)
    tweet = ["hello", "world"]
    tweetRdd = sc.parallelize(tweet)
    return tweetRdd



def wordCount(rdd, inputWord):
    rddTweetWord = rdd.filter(lambda x: x[0][1] == inputWord)
    return rddTweetWord.count()


def naiveBayes(rdd, inputTweet, rddCount):
    T = rddCount
    Tc = rdd.count()
    #Tc = TcRDD.count()
    #Tcw = TcRDD.filter(lambda x: x[10] if (x[10].find(word) for word in tweet))
    prob = float(T/Tc)
    for word in inputTweet:
        Tcw = wordCount(rdd, word)
        prob *= float(Tcw/Tc)
    return prob

def calculateProbabilities(rdd, inputTweet, rddCount):
    print rdd.take(5)
    print rdd.map(lambda x: (x[0][1], (x[0][0], x[1]))).take(5)
    print inputTweet.take(5)
    newRdd = rdd.map(lambda x: (x[0][1], (x[0][0], x[1]))).leftOuterJoin(inputTweet)
    print newRdd.collect()
    testRdd = newRdd.map(lambda x: ((x[1][0], x[0]), x[1][1]))
    #print testRdd.collect()
    places = testRdd.map(lambda x: x[0]).distinct().collect()
    probabilities = []
    '''rddWordCount = countRDD(rdd)
    for place in places:
        rddPlace = rddWordCount.filter(lambda x: x[0][0] == place)
        prob = naiveBayes(rddPlace, inputTweet, rddCount)
        probabilities.append(prob)
        print(place, ",", prob)
    maxProb = sc.parallellize(probabilities).max(key =lambda x: x[1])
    return maxProb'''

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
    training_rdd = sc.parallelize([(("NY", "hello"), 100), (("NY", "hi"), 50)])
    rddCount = 2
    #training_rdd, rddCount = createRDD(0.1)
    inputTweet = transformInputTweet()
    #rddWordCount = countRDD(training_rdd)
    calculateProbabilities(training_rdd, inputTweet, rddCount)



main()
