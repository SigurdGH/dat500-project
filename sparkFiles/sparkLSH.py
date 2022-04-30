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

# Get all articles in c2(Content) by taking each row
    # Need to collect here because we want to split the data across the workers
articles = df.select("_c2").rdd.map(lambda row: row._c2).collect()

articles = spark.sparkContext.parallelize(articles)

# Find the Most Common Words
split_articles = articles.flatMap( lambda article: list(ngrams(article.split(" "), 2)) ) # flatMap to avoid having multiple lists INSIDE of the list, we have one long list instead
count_shingles = split_articles.map(lambda shingle: (shingle, 1))
reduce_shingles = count_shingles.reduceByKey(lambda x, y: x+y )
sorted_shingles = reduce_shingles.sortBy(lambda keyValue: -keyValue[1]) # -keyValue to sort in descending(highest -> lowest) order
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


split_articles = articles.map( lambda article: list(ngrams(article.split(" "), 2)) )
binaries = split_articles.map(toBinaryMatrix).cache()
signatures = binaries.map(toSignMatrix).cache()
print(signatures.take(1))


# Turn Signature Matrix into LSH Buckets by using Bands
def toBands(signature):
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
    bucket = []

    bands_per_signature = data[0]
    index = data[1]

    for band in bands_per_signature:

        hashValue = ( hash(str(band)) % num_buckets) # Using string() convertion to be able to hash the vector to a bucket

        bucket.append((hashValue, index))
    return bucket
buckets = bands.flatMap(toLSHBuckets).groupByKey().mapValues(list).cache()
print(buckets.take(1))







