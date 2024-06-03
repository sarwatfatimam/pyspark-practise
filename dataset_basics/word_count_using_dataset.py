from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('word_count_dataset').getOrCreate()

# create a dataframe (an alias for dataset in PySpark)
df = spark.createDataFrame([("Hello world", ), ("Hello Spark", ), ("Hello PySpark",)], ['text'])

# word count using dataset
word_df = df.select(F.explode(F.split(F.col('text'), " ")).alias('word'))
word_df = word_df.groupby("word").count()

# Showing the result
print("Word counts using Dataset: ")
word_df.show()

spark.stop()
