from pyspark.sql import SparkSession

# initialize a spark session
spark = SparkSession.builder.appName('word_count_rdd').getOrCreate()

# Create rdd. Parallelize is used to distribute and process data in parallel.
text_rdd = spark.sparkContext.parallelize(["Hello world", "Hello Spark", "Hello PySpark"])

# word count using rdd
# flatMap creates another rdd with list of words splitted by space e.g.
# ["Hello", "world", "Hello", "Spark", "Hello", "PySpark"]
words_rdd = text_rdd.flatMap(lambda line: line.split(" "))
# map creates another rdd with tuples where each word is assigned 1 to it.
# [("Hello", 1), ("world", 1), ("Hello", 1) , ("Spark", 1), ("Hello", 1), ("PySpark", 1)]
# reduceByKey groups the values by keys and apply the lambda function a+b
# The groups would be like "Hello" : [1,1,1], "world": [1], "Spark": [1], "PySpark": [1]
# Now the lambda addition function will be applied as
# a=1, b=1 -> a+b = 2
# a=2, b=1 -> a+b = 3
word_counts_rdd = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

# The collect action retrieves the entire RDD as a list to the driver program. This is generally used for
# debugging purposes or when the RDD is small enough to fit in memory.
print("Word Counts using RDD: ")
print(word_counts_rdd.collect())


# We stop the Spark session to release the resources used by the application.
# This is a good practice to prevent resource leaks and to ensure the application ends cleanly.
spark.stop()
