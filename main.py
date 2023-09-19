from pyspark.sql import SparkSession
import part1 as part1
import part2 as part2
import part3 as part3


spark = SparkSession.builder \
    .appName("tp1_victor_e_arthur") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("==================== Iniciando script ====================\n")

dataset_path = "hdfs://localhost:9000/datasets/spotify/"
playlists_df = spark.read.json(dataset_path + 'playlist.json')
tracks_df = spark.read.json(dataset_path + 'tracks.json')

part1.songsDuration(sc, tracks_df)
part1.nonOutliersSongDurations(sc, tracks_df)
part2.run(sc, tracks_df, playlists_df)
part3.run(sc, tracks_df, playlists_df)

print("\n==================== Fim do script ====================\n")
sc.stop()
