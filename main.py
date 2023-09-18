from pyspark.sql import SparkSession
import part1 as part1
import part2 as part2
import part3 as part3


spark = SparkSession.builder \
    .appName("HelloLines") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("==================== Iniciando script ====================\n")

dataset_path = "hdfs://localhost:9000/datasets/spotify/"
tracks_df = spark.read.json(dataset_path + 'tracks.json')
playlists_df = spark.read.json(dataset_path + 'playlists.json')

part2.run(sc, dataset_path)

print("\n==================== Fim do script ====================\n")
sc.stop()
