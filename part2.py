# rdd = sc.textFile("hdfs:/user/victorribeiro/hello.txt")
# lines = rdd.count()
# outrdd = sc.parallelize([lines])
# # The following will fail if the output directory exists:
# outrdd.saveAsTextFile("hdfs:/user/victorribeiro/hello-linecount-submit")

def run(sc, dataset_path):
    dataset = ""