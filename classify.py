from pyspark import SparkContext, SparkConf

rdd = None
rdd_sample = None

conf = SparkConf()
sc = SparkContext(conf = conf)

# Creates an RDD from #5 place_name and #11 tweet_text
def createRDD(filename, val):
    rdd = sc.textFile(filename).map(lambda line: line.split('\t')).map(lambda x: x[4], x[10].lower().split(" ")))
    rdd_sample = rdd.sample(False, val, 5)
    return rdd_sample

# We need three parameters:
# -training <full path of the training file>
# -input <full path of the input file>
# -output <full path of the output file>
def run(rdd, tweet, outputFile):

# x = place, y = probability
# Can be more than one outcome
def writeFile(rdd, filename):
    # If there's one or more places found
    if rdd.count() > 0:
        rdd.map(lambda (x,y): x + "\t" + str(y)).coalesce(1).saveAsTextFile(filename)
    # If there's no place with prob greater than zero
    else:
        rdd.parallellize([]).coalesce(1).saveAsTextFile(filename)

def main():
    rdd = createRDD("./data/geotweets.tsv", 0.1)


main()
