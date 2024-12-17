import sys
import os
from pyspark.sql import SparkSession

def get_fs_root_folder(conf):
    fs_root = conf.get("hive.metastore.warehouse.dir")
    idx = fs_root.find("warehouse/tablespace/managed/hive")
    print("idx for the end of root folder: ", idx)
    fs_root = fs_root[:idx]
    print("there are {} arguments.".format(len(sys.argv)))
    if len(sys.argv) > 3:
        fs_root = sys.argv[3]
    if fs_root == "/":
        fs_root = "hdfs:///tmp/"
    return fs_root

def main():
    spark = SparkSession.builder.appName("SparkWriteParquet").getOrCreate()
    context = spark.sparkContext
    conf = context._jsc.hadoopConfiguration()
    fs_root = get_fs_root_folder(conf)
    print("file system root folder: ", fs_root)
    lines = spark.read.text(sys.argv[1])
    dest = os.path.join(fs_root, sys.argv[2])
    print("the destination of data file: ", dest)
    lines.write.mode("overwrite").parquet(dest)
    spark.stop()

if __name__ == "__main__":
    main()