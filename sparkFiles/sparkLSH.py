# Imports
from pyspark.sql import SparkSession, functions, types
from nltk import ngrams
import random
import numpy as np

# Set parameters
num_most_common_shingles = 5000 # More Words = More precise, but slower
num_hash_functions = 100 # More functions = more precise, but slower
num_band_rows = 10 # Tied to num_bands, relies on hash_functions
num_buckets = 100000 # An arbitrarily high number

# Start Session
spark = SparkSession.builder.appName("SparkML").getOrCreate()
spark.catalog.clearCache()

# Read the data
file = "file:///home/ubuntu/project/articles_10000.csv"
df = spark.read.csv(file, header=False, sep="|", multiLine=True, escape="\"")
#df.show()

# Get all articles in c2(Content) by taking each row
    # Need to collect here because we want to split the data across the workers
articles = df.select("_c2").rdd.map(lambda row: row._c2).collect()
#print(len(articles))
#articles.take(5)

articles = spark.sparkContext.parallelize(articles)

# Find the Most Common Words
#rddMostCommon = spark.sparkContext.parallelize(articles)
split_articles = articles.flatMap( lambda article: list(ngrams(article.split(" "), 2)) ) # flatMap to avoid having multiple lists INSIDE of the list, we have one long list instead
count_shingles = split_articles.map(lambda shingle: (shingle, 1))
reduce_shingles = count_shingles.reduceByKey(lambda x, y: x+y )
sorted_shingles = reduce_shingles.sortBy(lambda keyValue: -keyValue[1]) # -keyValue to sort in descending(highest -> lowest) order
# LEGG TIL FILTER FØR COLLECTING
#filter_shingles = sorted_shingles.filter(lambda keyValue: keyValue[1]>100000000)
#common_shingles = filter_shingles.saveAsTextFile("../../teitfil.txt")
#common_shingles = sorted_shingles.collect()
common_shingles = sorted_shingles.zipWithIndex().filter(lambda data: data[1]<num_most_common_shingles).collect()
print(common_shingles[0])


most_common_shingles = []
for word in common_shingles:
    most_common_shingles.append(word[0][0])

# Set the NONE value for the Signature Matrix
signature_none_value = len(most_common_shingles)+2 # +2 since we use 1 - len(data) in the sign matrix, could be any arbitrary high number

# Turn Articles into Binary Matrix into Signature Matrix
def toBinaryMatrix(data):
    binary = []
    for shingle in most_common_shingles: # Using non-local value
        if shingle in data:
            binary.append(1)
        else:
            binary.append(0)
    return binary

def generateHashes(num_hashes, value_range):
    values = list(range(1, value_range+1))
    hashes = []
    for i in range(num_hashes):
        hashes.append(random.sample(values, value_range))
    return hashes

hash_functions = generateHashes(num_hash_functions, num_most_common_shingles) # Using non-local value

def toSignMatrix(data):
    signature = []
    for hash_function in hash_functions:
        lowest_hash = signature_none_value # Using non-local value
        for idx, hash in enumerate(hash_function):
            if data[idx] == 1:
                if(hash < lowest_hash):
                    lowest_hash = hash
        signature.append(lowest_hash)
    return signature



### RESTRUCTURE ###
# binaries = split_articles.map(lambda article: toBinaryMatrix(article, PARAMETER VALUE))

# rddSignatureMatrix = spark.sparkContext.parallelize(articles)
split_articles = articles.map( lambda article: list(ngrams(article.split(" "), 2)) )
binaries = split_articles.map(toBinaryMatrix).cache()
signatures = binaries.map(toSignMatrix).cache()
print(signatures.take(1))


# Turn Signature Matrix into LSH Buckets by using Bands
def toBands(signature):
    #num_rows = 1
    band = []
    for i in range(0, len(signature), num_band_rows):
        band.append(signature[i:i+num_band_rows])
    return band
bands = signatures.map(toBands).zipWithIndex().cache()

# Since we are not using ALL words, there is a possibitilty that articles do not have words in them at all
    # We therefore have a safety measure that checks for NONE words and ignore these
        # Example; If we have two super short wierd language non-related articles, they would both have signature of 5
            # But they should not be in the same bucket, they are unrelated
def toLSHBuckets(data):
    #num_buckets = 1000
    bucket = []

    bands_per_signature = data[0]
    index = data[1]

    for band in bands_per_signature:

        hashValue = ( hash(str(band)) % num_buckets) # Using string() convertion to be able to hash the vector to a bucket

        bucket.append((hashValue, index))
    return bucket
buckets = bands.flatMap(toLSHBuckets).groupByKey().mapValues(list).cache()
print(buckets.take(1))


'''
def nearestNeighbors(data):
    nearest_neighbors = {}
    for key in data:
        if len(data[key]) > 1: # Only care about buckets that have several articles in them
            for article_id in data[key]:
                # Similar to the lsh buckets
                if article_id in nearest_neighbors.keys():
                    for doc_id in data[key]: # Looping over the same list again to add everything but myself (probably better ways to do this)
                        if doc_id != article_id:
                            nearest_neighbors[article_id].add(doc_id)
                        else:
                            nearest_neighbors[article_id] = set() # Using a set here, as the same article might be added twice, but then it's not counted
                            for doc_id in data[key]:
                                if doc_id != article_id:
                                    nearest_neighbors[article_id].add(doc_id)
    return nearest_neighbors
'''
# Split the bands across several nodes
#rddBuckets = spark.sparkContext.parallelize(bands)
#buckets = rddbuckets.map(toLSHBuckets)
#neighbors = buckets.map(nearestNeighbors).cache()
#print(neighbors.take(1))






