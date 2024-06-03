from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create a spark session
spark = SparkSession.builder.appName('average_product_rating').getOrCreate()

# read the file
df = spark.read.text('../data/product_reviews.txt')
df = df.select('*', F.split(F.col('value'), ",").getItem(0).alias('product'),
               F.split(F.col('value'), ",").getItem(1).alias('rating')).drop('value')
df = df.groupby(F.col('product')).agg(F.avg(F.col('rating')).alias('average_rating'))

print(df.show())
df.write.mode('overwrite').csv('./data/output_average_rating_df')

spark.stop()
