from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test HBase Connector from Python").getOrCreate()

hbase_cols = "name STRING :key, email STRING c:email, birthDate DATE p:birthDate, height FLOAT p:height"
hbase_tbl = "person"
df = spark.read \
    .format("org.apache.hadoop.hbase.spark") \
    .option("hbase.columns.mapping", hbase_cols) \
    .option("hbase.table", hbase_tbl) \
    .option("hbase.spark.use.hbasecontext", False) \
    .load()
df.createOrReplaceTempView("personView")
results = spark.sql("SELECT * FROM personView WHERE name = 'alice'")
results.show()
spark.stop()