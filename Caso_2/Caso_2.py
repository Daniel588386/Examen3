# Databricks notebook source
# MAGIC %md
# MAGIC # Encontrando insights de la UEFA Champions League

# COMMAND ----------

# MAGIC %md
# MAGIC La Liga de Campeones de la UEFA, a menudo llamada Liga de Campeones, es una competencia anual de fútbol por excelencia que cautiva a los fanáticos de todo el mundo. Establecida en 1955 como la Copa de Clubes Campeones de Europa, evolucionó a la Liga de Campeones de la UEFA en 1992, ampliando su atractivo. El formato moderno cuenta con 32 equipos de clubes de primer nivel seleccionados en función de su desempeño en la liga nacional, lo que aumenta la intriga.  
# MAGIC
# MAGIC Este evento electrizante trasciende los deportes y se convierte en una celebración de la unidad, la cultura y el orgullo nacional. Los fanáticos, vestidos con los colores de sus países, crean una atmósfera eléctrica, lo que hace que el torneo sea tanto sobre los espectadores como sobre los jugadores. Financieramente, la Liga de Campeones es un salvavidas para los clubes, ya que aumenta los ingresos y ofrece oportunidades transformadoras. Sin embargo, genera debates sobre las disparidades de riqueza en el fútbol europeo.  
# MAGIC
# MAGIC La Liga de Campeones es sinónimo de rivalidades históricas, triunfos de los desvalidos y brillantez individual. Para los jugadores, representa la cima de su carrera, mientras que para los fanáticos, es un fenómeno cultural. El himno y los rituales icónicos enriquecen la experiencia futbolística. En 200 palabras, la UEFA Champions League es el epítome de la excelencia del fútbol europeo, que ofrece momentos inolvidables, recompensas económicas y un impacto cultural único, con 32 clubes de primer nivel que se suman a su atractivo.

# COMMAND ----------

# MAGIC %md
# MAGIC **Nombres de las tablas o ficheros a utilizar**:  
# MAGIC [uefa_2020.csv](https://tajamar365.sharepoint.com/:x:/s/3405-MasterIA2024-2025/EQuW8d16tZJKpYZN6jR480sBgc1IYypAw9hGHKQARS560g?e=7TybBU)  
# MAGIC [uefa_2021.csv](https://tajamar365.sharepoint.com/:x:/s/3405-MasterIA2024-2025/ESG8YBrjD3tBky3aGT0MwIABNt_PQxFVVYVlWMnZYUmV8g?e=VrYHtX)  
# MAGIC [uefa_2022.csv](https://tajamar365.sharepoint.com/:x:/s/3405-MasterIA2024-2025/Ecf8O8U8sxJLoW9Bkd2ZVzgBb_gZc35mNcprnR9FMlzliQ?e=Vn3a6v)  
# MAGIC - Todas las tablas tienen las mismas columnas y data types

# COMMAND ----------

# MAGIC %md
# MAGIC | Column | Definition | Data type |
# MAGIC |--------|------------|-----------|
# MAGIC | `STAGE`| Stage of the March | `VARCHAR(50)` |
# MAGIC | `DATE` | When the match occurred. | `DATE` |
# MAGIC | `PENS` | Did the match end with penalty | `VARCHAR(50)` |
# MAGIC | `PENS_HOME_SCORE` | In case of penalty, score by home team | `VARCHAR(50)` |
# MAGIC | `PENS_AWAY_SCORE` | In case of penalty, score by away team | `VARCHAR(50)` |
# MAGIC | `TEAM_NAME_HOME` | Team home name | `VARCHAR(50)` |
# MAGIC | `TEAM_NAME_AWAY`| Team away  name | `VARCHAR(50)` |
# MAGIC | `TEAM_HOME_SCORE` | Team home score | `NUMBER` |
# MAGIC | `TEAM_AWAY_SCORE` | Team away score | `NUMBER` |
# MAGIC | `POSSESSION_HOME` | Ball possession for the home team | `FLOAT` |
# MAGIC | `POSSESSION_AWAY` | Ball possession for the away team | `FLOAT` |
# MAGIC | `TOTAL_SHOTS_HOME` | Number of shots by the home team | `NUMBER` |
# MAGIC | `TOTAL_SHOTS_AWAY` | Number of shots by the away team | `NUMBER`
# MAGIC | `SHOTS_ON_TARGET_HOME` | Total shot for home team | `FLOAT` |
# MAGIC | `SHOTS_ON_TARGET_AWAY` | Total shot for away team | `FLOAT` |
# MAGIC | `DUELS_WON_HOME` | duel win possession of ball - for home team | `NUMBER` |
# MAGIC | `DUELS_WON_AWAY` | duel win possession of ball - for away team | `NUMBER` 
# MAGIC | `PREDICTION_TEAM_HOME_WIN` | Probability of home team to win | `FLOAT` |
# MAGIC | `PREDICTION_DRAW` | Probability of draw | `FLOAT` |
# MAGIC | `PREDICTION_TEAM_AWAY_WIN` | Probability of away team to win | `FLOAT` |
# MAGIC | `LOCATION` | Stadium where the match was held | `VARCHAR(50)` |

# COMMAND ----------

from pyspark.sql import functions as F

df_2020 = spark.read.option("header", "true").csv("dbfs:/FileStore/EP_2/uefa_2020.csv")
df_2021 = spark.read.option("header", "true").csv("dbfs:/FileStore/EP_2/uefa_2021.csv")
df_2022 = spark.read.option("header", "true").csv("dbfs:/FileStore/EP_2/uefa_2022.csv")

df_2020 = df_2020.withColumn("year", F.lit(2020))
df_2021 = df_2021.withColumn("year", F.lit(2021))
df_2022 = df_2022.withColumn("year", F.lit(2022))

df = df_2020.union(df_2021).union(df_2022)
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Requerimientos:  
# MAGIC -Puedes utilizar SQL o PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Encuentra los 3 equipos que anotaron más goles jugando en su estadio en la UEFA Champions League 2020-21. El resultado debe contener dos columnas: TEAM_NAME_HOME y TEAM_HOME_SCORE ordenadas en orden descendente de TEAM_HOME_SCORE. Guarda la consulta (o el notebook) como TEAM_HOME_WITH_MOST_GOALS.

# COMMAND ----------

# DBTITLE 1,TEAM_HOME_WITH_MOST_GOALS.
from pyspark.sql import functions as F

df_home_goals = df.filter(df["TEAM_HOME_SCORE"].isNotNull())

df_home_goals_sum = df_home_goals.groupBy("TEAM_NAME_HOME").agg(
    F.sum("TEAM_HOME_SCORE").alias("TOTAL_GOALS_HOME")
)

df_home_goals_sorted = df_home_goals_sum.orderBy(F.desc("TOTAL_GOALS_HOME"))

top_teams_home_goals = df_home_goals_sorted.limit(3)

top_teams_home_goals.select("TEAM_NAME_HOME", "TOTAL_GOALS_HOME").display()

top_teams_home_goals.write.format("parquet").save("dbfs:/FileStore/TEAM_HOME_WITH_MOST_GOALS")


# COMMAND ----------

# MAGIC %md
# MAGIC 2. Encuentra el equipo con posesión mayoritaria la mayor cantidad de veces durante la UEFA Champions League 2021-22. El resultado debe incluir dos columnas: TEAM_NAME y GAME_COUNT, que es la cantidad de veces que el equipo tuvo posesión mayoritaria durante un partido de fútbol. Guarda esta consulta (o el notebook) como TEAM_WITH_MAJORITY_POSSESSION

# COMMAND ----------

# DBTITLE 1,TEAM_WITH_MAJORITY_POSSESSION
from pyspark.sql import functions as F

df_2021_2022 = df.filter(df["year"] == 2022)

df_home_majority_possession = df_2021_2022.filter(df_2021_2022["POSSESSION_HOME"] > df_2021_2022["POSSESSION_AWAY"])

df_away_majority_possession = df_2021_2022.filter(df_2021_2022["POSSESSION_AWAY"] > df_2021_2022["POSSESSION_HOME"])

df_majority_possession = df_home_majority_possession.select("TEAM_NAME_HOME").withColumnRenamed("TEAM_NAME_HOME", "TEAM_NAME")
df_majority_possession_away = df_away_majority_possession.select("TEAM_NAME_AWAY").withColumnRenamed("TEAM_NAME_AWAY", "TEAM_NAME")

df_all_majority_possession = df_majority_possession.union(df_majority_possession_away)

df_team_majority_possession_count = df_all_majority_possession.groupBy("TEAM_NAME").count()

df_team_majority_possession_count = df_team_majority_possession_count.withColumnRenamed("count", "GAME_COUNT")

df_team_majority_possession_sorted = df_team_majority_possession_count.orderBy(F.desc("GAME_COUNT"))

df_team_majority_possession_sorted.display()

df_team_majority_possession_sorted.write.format("parquet").save("dbfs:/FileStore/TEAM_WITH_MAJORITY_POSSESSION")


# COMMAND ----------

# MAGIC %md
# MAGIC 3. Encuentra la lista de equipos de cada fase del juego que ganaron el duelo en un partido pero terminaron perdiendo el juego en el Campeonato de la UEFA 2022-23. El resultado debe contener dos columnas: STAGE y TEAM_LOST. Guarda la consulta (o el notebook) como TEAM_WON_DUEL_LOST_GAME_STAGE_WISE.

# COMMAND ----------


