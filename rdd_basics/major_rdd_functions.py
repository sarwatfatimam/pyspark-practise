from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('major_functions').getOrCreate()

# Transformations
# map(func): applies a function to each element of the RDD and returns a new RDD
rdd1 = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rdd_map = rdd1.map(lambda x: x * 4)
print('map(func): ', rdd_map.collect())

# flatMap(func): similar to 'map' but each input item can be mapped to 0 or more output items (flattening the result)
rdd2 = spark.sparkContext.parallelize(['Hello World', 'Hello Spark', 'Hello PySpark'])
rdd_flatmap = rdd2.flatMap(lambda x: x.split(" "))
print('flatMap(func): ', rdd_map.collect())

# filter(func): returns a new RDD containing only the elements that satisfy the condition
rdd3 = spark.sparkContext.parallelize([1, 2, 4, 8, 9, 10])
rdd_filter = rdd3.filter(lambda x: x % 2 == 0)
print('filter(func): ', rdd_filter.collect())

# distinct(): returns a new RDD containing the distinct elements of the original RDD
rdd4 = spark.sparkContext.parallelize([1, 1, 1, 2, 2, 2])
rdd_distinct = rdd4.distinct()
print('distinct(): ', rdd_distinct.collect())

# reduceByKey(func): Aggregates values for each key using a function for key-value pairs
rdd5 = spark.sparkContext.parallelize([("a", 1), ("b", 2), ("b", 3)])
rdd_red = rdd5.reduceByKey(lambda x, y: x + y)
print('reduceByKey(func): ', rdd_red.collect())

# groupByKey(): group values for each key using a function for key-value pairs
rdd6 = spark.sparkContext.parallelize([("a", 1), ("b", 2), ("b", 3)])
rdd_grp_tup = rdd6.groupByKey().mapValues(tuple)
rdd_grp_list = rdd6.groupByKey().mapValues(list)
print('groupByKey: ', rdd_grp_tup.collect())
print('groupByKey: ', rdd_grp_list.collect())

# sortByKey(ascending=True): sorts an rdd by key
rdd7 = spark.sparkContext.parallelize([('a', 2), ('b', 3), ('c', 4)])
rdd_sort = rdd7.sortByKey(ascending=False)
print('sortByKey: ', rdd_sort.collect())

# join(other): joins two rdds based on key
rdd8 = spark.sparkContext.parallelize([('a', 1), ('b', 2)])
rdd9 = spark.sparkContext.parallelize([('a', 3), ('b', 4)])
rdd_join = rdd8.join(rdd9)
print('join(other): ', rdd_join.collect())

# Actions
# collect: returns all elements of the RDD as an array at the driver program
print('collect:', rdd8.collect())

# count: returns the number of elements in the RDD
print('count: ', rdd_join.count())

# take(n): returns the first n elements of an RDD
print('take(n): ', rdd1.take(2))

# reduce(func): aggregates the elements of the RDD based on a specific function
print('reduce(func): ', rdd1.reduce(lambda a, b: a+b))

# saveAsTextFile(path): saves an RDD at a specified path
rdd1.saveAsTextFile('../data/saving_rdd')

# countByKey(): count the number of element for each key for key-value pair
print('countByKey(): ', rdd5.countByKey())
print('countByValue(): ', rdd5.countByValue())

# foreach(func): applies a function to each element of the RDD
print('foreach(func): ', rdd1.foreach(lambda x: x*2))






