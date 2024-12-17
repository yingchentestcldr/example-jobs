from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.json("file:///opt/spark/examples/src/main/resources/people.json")
df.show()
print(spark.version)
spark.stop()