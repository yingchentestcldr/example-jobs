# using delegation token
import sys
from pyspark.sql import SparkSession

kafka_brokers = sys.argv[1]
topic = sys.argv[2]
spark = SparkSession.builder \
    .appName("read kafka topic in batch mode") \
    .getOrCreate()

df = spark.read \
    .format("kafka") \
    .option("subscribe", topic) \
    .option("startingOffsets", '''{"'''+topic+'''":{"0":-2}}''') \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("kafka.sasl.jaas.config", 
            'org.apache.kafka.common.security.scram.ScramLoginModule required debug=true;') \
    .load()
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
data = query.select("value")
data.printSchema()
print(data.count())
data.show()
spark.stop()