from pyspark.sql import SparkSession
import os

if os.name == 'nt':
    print('detected windows, setting HADOOP_HOME')
    os.environ['HADOOP_HOME'] = 'C:/hadoop/hadoop-2.7.1'

spark = SparkSession.builder \
    .master("{URL}") \
    .appName("Estimate PI") \
    .getOrCreate()
sc = spark.sparkContext
NUM_SAMPLES = 100000
