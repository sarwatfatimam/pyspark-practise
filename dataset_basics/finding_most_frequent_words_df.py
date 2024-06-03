from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('frequent_words').getOrCreate()

df = spark.read.text('../data/frequent_words.txt')

df = df.withColumn('value_list', F.split(F.lower(F.col('value')), ' '))
df = df.withColumn('words', F.explode(F.col('value_list')))
df = df.groupby('words').agg(F.count('words').alias('count')).orderBy(F.col('count').desc())
df = df.filter(F.col('count') > 1)

print(df.show())
