from pyspark import SparkContext, SparkConf
import argparse

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")

parser = argparse.ArgumentParser(description="Classification using Naive Bayes.")

parser.add_argument('-training', '-t', dest='training', default='./data/geotweets.tsv')
parser.add_argument('-input', '-i', dest='inputTweet')
parser.add_argument('-output', '-o', dest='outputFile', default='./outputFile.tsv')

args = parser.parse_args()

# originalRdd consists of elements containing place and a list of words in tweet: [(place, [w1, w2, ...]), ...]
'''orgRdd = sc.textFile(args.training, use_unicode = True)\
        .map(lambda line: line.split('\t')).map(lambda x: (x[4], x[10].lower().split(" "))).sample(False, 0.00001, 5)
'''
orgData = [("New York, NY", "empire state building"),
           ("New York, NY", "big empire state"),
           ("New York, NY", "new york state"),
           ("London, UK", "big ben"),
           ("London, UK", "big state building")]

orgRdd = sc.parallelize(orgData)
originalRdd = orgRdd.map(lambda x: (x[0], x[1].lower().split(" ")))

# T = total number of tweets
T = originalRdd.count()
# places = rdd of distinct places
places = originalRdd.map(lambda x: x[0]).distinct()
# placeCount = rdd of [(place, number of tweets from place)]
placeCount = originalRdd.map(lambda x: (x[0], 1)).reduceByKey(
    lambda x, y: x + y)

# wordCountRdd = rdd of [((place, word), wordcount),...]
wordCountRdd = originalRdd.flatMap(lambda x: ((x[0], word) for word in x[1])) \
    .map(lambda x: ((x[0], x[1]), 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False)

# wordCount2Rdd = rdd of [(word, (place, wordcount)), ...]
wordCount2Rdd = wordCountRdd.map(lambda x: (x[0][1], (x[0][0], x[1])))

def transformInputTweet():
    # tweet = tweetText.lower().split(" ")
    # tweetTrans = sc.textFile(args.inputTweet, use_unicode = True)
    tweet = ["state", "building", "building", "building"]
    tweetRdd = sc.parallelize(tweet).map(lambda word: (word, 0))
    return tweetRdd


def Tc(place):
    tc = placeCount.filter(lambda x: x[0] == place).map(lambda x: x[1]).collect()
    return float(tc[0])


def Tcw(inputTweetRdd, place):
    tc = Tc(place)
    #join consists now of [(word, (place, wordcount)]
    join = inputTweetRdd.leftOuterJoin(wordCount2Rdd)
    # tcw = [(word, place), tcw)...], where tcw = wordcount/tc
    tcwRdd = inputTweetRdd.leftOuterJoin(wordCount2Rdd).map(lambda x: ((x[0], x[1][1][0]), x[1][1][1])).filter(lambda x: x[0][1] == place)
    #line 64: should filter on place before mapping, but haven't been able to make it work yet
    tcwList = tcwRdd.map(lambda x: x[1]).collect()
    #@TODO: handle None-case:
    #test = join.map(lambda x:( x[1][1] == ('XXX') if x[1][1] == None else x[1][1]))
    return tcwList

def prob(inputTweetRdd,place):
    tc = Tc(place)
    tcw = Tcw(inputTweetRdd, place)
    prob = (tc / T)
    for x in range(0, len(tcw)):
        prob *= (tcw[x] / tc)
    print prob
    return prob

# x = place, y = probability
# Can be more than one outcome
def writeFile(rdd):
    # If there's one or more places found
    if rdd.count() > 0:
        rdd.map(lambda (x, y): x + "\t" + str(y)).coalesce(1).saveAsTextFile(args.outputFile)
    # If there's no place with prob greater than zero
    else:
        results = sc.parallellize([])
        results.coalesce(1).saveAsTextFile(args.outputFile)


def main():
    #place = "London, UK"
    place = "New York, NY"
    inputTweetRdd = transformInputTweet()
    Tc(place)
    prob(inputTweetRdd, place)

    #@TODO: Need to handle None-case where tweetwords does not exist.
    #  Need to change prob() to not use place as parameter, but find maximum p(c) instead.
main()
