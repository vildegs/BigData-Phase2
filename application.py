from pyspark import SparkContext, SparkConf

rdd = None
rdd_sample = None

conf = SparkConf()
sc = SparkContext(conf = conf)

def createRDD(filename, val):
    rdd = sc.textFile(filename).map(lambda line: line.split('\t'))
    rdd_sample = rdd.sample(False, val, 5)
    return rdd_sample

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
