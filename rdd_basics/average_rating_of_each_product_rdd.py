from pyspark.sql import SparkSession

# Create a spark session
spark = SparkSession.builder.appName('average_product_rating').getOrCreate()

# read the file
rdd_df = spark.sparkContext.textFile('./data/product_reviews.txt')
rdd_df = rdd_df.map(lambda v: v.split(', '))
rdd_df = rdd_df.map(lambda v: (v[0], (int(v[1]), 1)))
rdd_df = rdd_df.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
rdd_df = rdd_df.map(lambda product: (product[0], product[1][0]/product[1][1]))

print(rdd_df.collect())
rdd_df.saveAsTextFile('./data/output_product_average_rating_rdd')

spark.stop()
