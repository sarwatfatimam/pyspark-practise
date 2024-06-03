from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('frequent_words').getOrCreate()

rdd_df = spark.sparkContext.textFile('../data/frequent_words.txt')

# Solution 1:
rdd_split = rdd_df.map(lambda x: x.split(' '))
rdd_flatmap = rdd_split.flatMap(lambda x: [i for i in x])
rdd_count = rdd_flatmap.map(lambda x: (x, 1))
rdd_key_count = rdd_count.reduceByKey(lambda a, b: a + b)
rdd_filter = rdd_key_count.filter(lambda x: x[1] > 1)

print(rdd_filter.collect())

# Solution 2:

# Read the file into an RDD
text_rdd = spark.sparkContext.textFile("../data/frequent_words.txt")

# Process the RDD to find the most frequent words
word_counts_rdd = (text_rdd
    .flatMap(lambda line: line.split(" "))
    .map(lambda word: (word.lower(), 1))
    .reduceByKey(lambda a, b: a + b)
)

# Sort by frequency and take the top 10
top_words_rdd = word_counts_rdd.sortBy(lambda x: x[1], ascending=False).take(10)

# Print the results
print("Most frequent words using RDD:")
for word, count in top_words_rdd:
    print(f"{word}: {count}")

# Stop Spark session
spark.stop()
