# using delegation token
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("read/write kafka topic in streaming mode") \
    .getOrCreate()

df = spark.createDataFrame([{'value': 'hello'}, {'value': 'world'}])
df.printSchema()
df.show()
data = df.select("value")
data.printSchema()
data.show()

data.write \
    .format("kafka") \
    .option("topic", "test") \
    .option("kafka.bootstrap.servers",
            "ccycloud-1.xhu-718.root.comops.site:9093,ccycloud-3.xhu-718.root.comops.site:9093,ccycloud-2.xhu-718.root.comops.site:9093") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.scram.ScramLoginModule required debug=true;') \
    .option("kafka.enable.idempotence", 'false') \
    .save()