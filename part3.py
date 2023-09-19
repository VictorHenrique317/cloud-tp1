from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import matplotlib.pyplot as plt

def run(sc, tracks_df, playlists_df):
    artist_counts = tracks_df.groupBy('pid', 'artist_name').count()

    window = Window.partitionBy('pid').orderBy(col('count').desc())
    most_frequent_artists = artist_counts.withColumn('rank', row_number().over(window)).filter(col('rank') == 1).drop('rank')

    total_tracks = tracks_df.groupBy('pid').count().withColumnRenamed('count', 'total')
    most_frequent_artists = most_frequent_artists.join(total_tracks, 'pid')
    most_frequent_artists = most_frequent_artists.withColumn('prevalence', col('count') / col('total'))

    pdf = most_frequent_artists.toPandas()

    pdf['prevalence'].hist(cumulative=True, density=1, bins=100)
    plt.xlabel('Artist Prevalence')
    plt.ylabel('CDF')
    plt.grid(True)
    plt.savefig("./part3.png")
