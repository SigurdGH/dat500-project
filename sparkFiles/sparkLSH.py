# Imports
from pyspark.sql import SparkSession, functions, types
from nltk import ngrams
import random
import numpy as np
import re
from pyspark.mllib.linalg import SparseVector

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

# Bring the data to the driver
articles = df.select("_c2").rdd.map(lambda row: row._c2).collect()

# Clean up the article and create shingles
def nGrams(data):
    filter = re.sub(r"[^a-zA-Z0-9 ]+", '', data)
    filterToLower = filter.lower()
    shingles = list(ngrams(filterToLower.split(" "), 2))
    return shingles

# Split the articles across the machines
articles = spark.sparkContext.parallelize(articles)

# Find the Most Common Words
split_articles = articles.flatMap(nGrams) # flatMap to avoid having multiple lists INSIDE of the list, we have one long list instead
count_shingles = split_articles.map(lambda shingle: (shingle, 1))
reduce_shingles = count_shingles.reduceByKey(lambda x, y: x+y )
sorted_shingles = reduce_shingles.sortBy(lambda keyValue: -keyValue[1]) # -keyValue to sort in descending(highest -> lowest) order
common_shingles = sorted_shingles.zipWithIndex().filter(lambda data: data[1]<num_most_common_shingles).collect()

most_common_shingles = []
for word in common_shingles:
    most_common_shingles.append(word[0][0])


# Turn Articles into Binary Matrix into Signature Matrix
def toBinaryMatrix(data):
    binary = []
    for shingle in most_common_shingles: 
        if shingle in data:
            binary.append(1)
        else:
            binary.append(0)
    sparse = SparseVector(len(binary), np.nonzero(binary), np.ones(len(np.nonzero(binary))))
    return sparse

def generateHashes(num_hashes, value_range):
    values = list(range(0, value_range))
    hashes = []
    for i in range(num_hashes):
        hashes.append(random.sample(values, value_range))
    return hashes

hash_functions = generateHashes(num_hash_functions, num_most_common_shingles)

def toSignMatrix(data):
    binary = data.toArray()
    signature = []
    for hash_function in hash_functions:
        lowest_hash = len(binary)+1
        for idx, hash in enumerate(hash_function):
            if binary[idx] == 1:
                if(hash < lowest_hash):
                    lowest_hash = hash
        signature.append(lowest_hash)
    return signature

split_articles = articles.map(nGrams)
binaries = split_articles.map(toBinaryMatrix).cache()
signatures_matrix = binaries.map(toSignMatrix).collect()
signatures = spark.sparkContext.parallelize(signatures_matrix)


# Turn Signature Matrix into LSH Buckets by using Bands
def toBands(signature):
    band = []
    for i in range(0, len(signature), num_band_rows):
        band.append(signature[i:i+num_band_rows])
    return band
bands = signatures.map(toBands).zipWithIndex().cache()

# Bands to buckets
def toLSHBuckets(data):
    bucket = []

    bands_per_signature = data[0]
    index = data[1]

    for band in bands_per_signature:
        hashValue = ( hash(str(band)) % num_buckets) # Using string() convertion to be able to hash the vector to a bucket
        bucket.append((hashValue, index))
        
    return bucket
buckets = bands.flatMap(toLSHBuckets).groupByKey().mapValues(list).cache()

# Buckets to neighbors
def getJaccardSimilarityScore(C1, C2):
    rows = len(C1)
    similarities = 0
    for i in range(rows):
        if C1[i] == C2[i]:
            similarities += 1
    
    return (similarities / rows)

def mapArticleNeighbors(bucket):
    neighbors = bucket[1]
    similar_ids = []
    
    for current_id in neighbors:
        for ids in neighbors:
            if ids != current_id:
                score = getJaccardSimilarityScore(signatures_matrix[ids], signatures_matrix[current_id])
                if score >= threshold:
                    pair_tuple = (current_id, ids)
                    similar_ids.append(pair_tuple)
        
    return similar_ids

neighbors = buckets.flatMap(mapArticleNeighbors).cache()
neighbors.take(1)






