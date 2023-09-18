from pyspark.sql import SparkSession
import part1 as part1
import part2 as part2
import part3 as part3


spark = SparkSession.builder \
    .appName("HelloLines") \
    .getOrCreate()
sc = spark.sparkContext

# rdd = sc.textFile("hdfs:/user/victorribeiro/hello.txt")
# lines = rdd.count()
# outrdd = sc.parallelize([lines])
# # The following will fail if the output directory exists:
# outrdd.saveAsTextFile("hdfs:/user/victorribeiro/hello-linecount-submit")

dataset_path = "hdfs://localhost:9000/datasets/spotify/"

part2.run(sc, dataset_path)


sc.stop()
