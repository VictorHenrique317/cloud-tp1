import pyspark.sql.functions as F
import pandas as pd
import matplotlib.pyplot as plt

# Gera uma tabela com a duração mínima, média e máxima em milissegundos
def songsDuration(sc, tracks_df):
    print("Parte 1a:\n")

    # Calcula a duração mínima, média e máxima das músicas
    songs_duration = tracks_df.agg(
        F.min("duration_ms").alias("min"),
        F.avg("duration_ms").alias("avg"),
        F.max("duration_ms").alias("max")
    )

    # Converte para Pandas DataFrame
    pd_df = songs_duration.toPandas()

    # Exibe a tabela com a duração das músicas
    print(pd_df)

    # Salva a tabela como um arquivo CSV
    pd_df.to_csv("./songs_duration_table.csv", index=False)

# Gera uma tabela com estatísticas da duração das músicas sem outliers
def nonOutliersSongDurations(sc, tracks_df):
    print("Parte 1b:\n")

    quartils = tracks_df.agg(
        F.min("duration_ms").alias("min (ms)"),
        F.expr("percentile_approx(duration_ms, 0.25)").alias("q1"),
        F.avg("duration_ms").alias("avg (ms)"),
        F.expr("percentile_approx(duration_ms, 0.75)").alias("q3"),
        F.max("duration_ms").alias("max (ms)")
    )

    iqr = quartils.select((quartils.q3 - quartils.q1).alias("iqr"))
    quartils = quartils.join(iqr)

    # Extrai os valores de Q1 e Q3
    q1 = quartils.select("q1").first()[0]
    q3 = quartils.select("q3").first()[0]

    # Calcula os limites para identificar outliers usando a metodologia IQRR
    iqr_value = q3 - q1
    lower_bound = q1 - 1.5 * iqr_value
    upper_bound = q3 + 1.5 * iqr_value

    # Filtra as músicas que não são outliers
    non_outliers_songs = tracks_df.filter((tracks_df.duration_ms >= lower_bound) & (tracks_df.duration_ms <= upper_bound))

    # Calcula o número de músicas consideradas outliers
    total_songs = tracks_df.count()
    outliers_count = total_songs - non_outliers_songs.count()

    print("Número de músicas consideradas outliers e removidas da análise (usando IQRR):", outliers_count)

    # Converte para Pandas DataFrame
    pd_df = non_outliers_songs.agg(
        F.min("duration_ms").alias("min_duration"),
        F.avg("duration_ms").alias("avg_duration"),
        F.max("duration_ms").alias("max_duration")
    ).toPandas()

    # Exibe a tabela com as estatísticas da duração das músicas sem outliers
    print(pd_df)

    # Salva a tabela como um arquivo CSV
    pd_df.to_csv("./remaining_stats.csv", index=False)

    return non_outliers_songs