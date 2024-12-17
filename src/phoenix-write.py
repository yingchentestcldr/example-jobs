from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test Phoenix-Spark Connector from Python").getOrCreate()

df = spark.createDataFrame([
    (1, '1')
], schema='a long, b string')
df.write \
  .format("org.apache.phoenix.spark") \
  .mode("overwrite") \
  .option("table", "TABLE1") \
  .option("zkUrl", "ccycloud-1.xhu-718.root.comops.site:2181") \
  .save()
df.show()
spark.stop()