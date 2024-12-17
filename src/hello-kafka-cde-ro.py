# using delegation token
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("read kafka topic in streaming mode") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("subscribe", "test") \
    .option("kafka.bootstrap.servers", 
            "ccycloud-1.xhu-718.root.comops.site:9093,ccycloud-3.xhu-718.root.comops.site:9093,ccycloud-2.xhu-718.root.comops.site:9093") \
    .option("kafka.sasl.jaas.config", 
            'org.apache.kafka.common.security.scram.ScramLoginModule required debug=true;') \
    .load()
df.printSchema()
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
data = query.select("value")
data.writeStream \
    .format("console") \
    .start() \
    .awaitTermination()