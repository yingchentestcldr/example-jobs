import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test Phoenix-Spark Connector from Python").getOrCreate()

zk_url = sys.argv[1]
tbl = sys.argv[2]
df = spark.read \
  .format("org.apache.phoenix.spark") \
  .option("table", tbl) \
  .option("zkUrl", zk_url) \
  .load()
df.show()
spark.stop()