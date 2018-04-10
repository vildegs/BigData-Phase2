from pyspark import SparkContext, SparkConf

rdd = None
rdd_sample = None

conf = SparkConf()
sc = SparkContext(conf = conf)

def createRDD(filename, val):
    rdd = sc.textFile(filename).map(lambda line: line.split('\t'))
    rdd_sample = rdd.sample(False, val, 5)
    return rdd_sample

def main():
    rdd = createRDD("./data/geotweets.tsv", 0.1)
    run("./result.tsv", rdd)

main()
