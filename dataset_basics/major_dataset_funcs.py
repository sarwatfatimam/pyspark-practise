from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('major_dataset_funcs').getOrCreate()

df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], ('name', 'age'))

# select: selects a subset of columns
df1 = df.select('name')
df1.show()

# withColumn: Adds a new column or replaces an existing columns
df2 = df.withColumn('age', (F.col('age') + F.lit(40)))
df2.show()

# filter (cond): filters dataframe based on the condition
df3 = df2.filter(df2['age'] > 42)
df3.show()

# groupBy(cols) : group rows by specified colus
df4 = df2.groupBy('name').avg('age')
df4.show()

# agg(exprs) : calculate aggregate statistics for grouped data
df5 = df2.groupBy('name').agg({'age': 'avg'})
df5.show()



