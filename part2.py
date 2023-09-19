from pyspark.sql.functions import countDistinct, from_unixtime, year
import matplotlib.pyplot as plt

def run(sc, tracks_df, playlists_df):
    print("Parte 2: \n")
    joined_df = tracks_df.join(playlists_df, "pid")
    joined_df = joined_df.withColumn("modified_at", from_unixtime("modified_at"))

    top_artists = joined_df.groupBy("artist_name").agg(countDistinct("pid").alias("num_playlists")).orderBy("num_playlists", ascending=False).limit(5)

    top_artists.show()

    for row in top_artists.collect():
        artist_name = row["artist_name"]
        artist_data = joined_df.filter(joined_df["artist_name"] == artist_name).groupBy(year("modified_at").alias("year")).count().orderBy("year")
        artist_data_pd = artist_data.toPandas()
        plt.plot(artist_data_pd["year"], artist_data_pd["count"], label=artist_name)

    plt.xlabel('Year')
    plt.ylabel('Number of Playlists')
    plt.title('Number of Playlists Containing Top Artists Over the Years')
    plt.legend()
    plt.grid(True)

    plt.savefig("./part2.png")