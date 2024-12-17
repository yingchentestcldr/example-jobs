from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, regexp_replace
import sys
import os

def get_fs_root_folder(conf):
    fs_root = conf.get("hive.metastore.warehouse.dir")
    idx = fs_root.find("warehouse/tablespace/managed/hive")
    print("idx for the end of root folder: ", idx)
    fs_root = fs_root[:idx]
    print("there are {} arguments.".format(len(sys.argv)))
    if len(sys.argv) > 1:
        fs_root = sys.argv[1]
    if fs_root == "/":
        fs_root = "hdfs:///tmp/"
    return fs_root

spark = SparkSession \
    .builder \
    .appName("Pyspark Tokenize") \
    .getOrCreate()

print("Spark version from spark.version: {}".format(spark.version))
print("Spark version from spark.sparkContext.version: {}".format(spark.sparkContext.version))
ver = spark.version
if ver[0] == '2':
    print("you are running Spark 2 and we don't support iceberg in Spark 2.")
    spark.stop()
    exit(0)

context = spark.sparkContext
conf = context._jsc.hadoopConfiguration()
fs_root = get_fs_root_folder(conf)
print("file system root folder: ", fs_root)

input_path ='example-data/access-logs.txt'
input_path = os.path.join(fs_root, input_path)
print("the source data location: {}".format(input_path))
base_df=spark.read.text(input_path)

split_df = base_df.select(regexp_extract('value', r'([^ ]*)', 1).alias('ip'),
                          regexp_extract('value', r'(\d\d\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})', 1).alias('date'),
                          regexp_extract('value', r'^(?:[^ ]*\ ){6}([^ ]*)', 1).alias('url'),
                          regexp_extract('value', r'(?<=product\/).*?(?=\s|\/)', 0).alias('productstring')
                         )

filtered_products_df = split_df.filter("productstring != ''")
cleansed_products_df=filtered_products_df.select(regexp_replace("productstring", "%20", " ").alias('product'), "ip", "date", "url")

user = os.environ.get('SPARK_USER')
user = user.replace('.', '_')
db_name = "retail_{}".format(user)
print("Creating Database: {} \n".format(db_name))
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(db_name))

table_name = "tokenized_iceberg"
print(f"Inserting Data into retail.tokenized_iceberg table \n")
cleansed_products_df.\
  writeTo("example_jobs.{}.tokenized_iceberg1".format(db_name)).createOrReplace()

print(f"=====show iceberg table metadata=====")
print(f"show iceberg table history")
spark.read.table("example_jobs.{}.tokenized_iceberg1.history".format(db_name)).show()
print(f"show iceberg table files")
spark.read.table("example_jobs.{}.tokenized_iceberg1.files".format(db_name)).show()
print(f"show iceberg table manifests")
spark.read.table("example_jobs.{}.tokenized_iceberg1.manifests".format(db_name)).show()
print(f"show iceberg table snapshots")
spark.read.table("example_jobs.{}.tokenized_iceberg1.snapshots".format(db_name)).show()
print(f"=====end of iceberg table metadata=====")


print(f"Count number of records inserted \n")
spark.sql("Select count(*) as RecordCount from example_jobs.{}.tokenized_iceberg1".format(db_name)).show()

print(f"Retrieve 15 records for validation \n")
spark.sql("Select * from example_jobs.{}.tokenized_iceberg1 limit 15".format(db_name)).show()

print(f"Calcualte the max among the timestamp \n")
spark.sql("select cast (from_unixtime(unix_timestamp(date,'dd/MMM/yyyy:HH:mm'), 'yyyy-MM-dd HH:mm') as String) as date from example_jobs.{}.tokenized_iceberg1".format(db_name)).show()
spark.stop()