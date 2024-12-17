# using delegation token
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("read/write kafka topic in streaming mode") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("subscribe", "test") \
    .option("startingOffsets", """{"test":{"0":-2}}""") \
    .option("kafka.bootstrap.servers", 
            "ccycloud-1.xhu-718.root.comops.site:9093,ccycloud-3.xhu-718.root.comops.site:9093,ccycloud-2.xhu-718.root.comops.site:9093") \
    .option("kafka.sasl.jaas.config", 
            'org.apache.kafka.common.security.scram.ScramLoginModule required debug=true;') \
    .load()
df.printSchema()
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
data = query.select("value")
checkpoint = "/tmp/hive/cdp_xhu/checkpoint"

data.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("topic", "test") \
    .option("checkpointLocation", checkpoint) \
    .option("kafka.bootstrap.servers",
            "ccycloud-1.xhu-718.root.comops.site:9093,ccycloud-3.xhu-718.root.comops.site:9093,ccycloud-2.xhu-718.root.comops.site:9093") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.scram.ScramLoginModule required debug=true;') \
    .start().awaitTermination()