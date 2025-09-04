# Databricks notebook source

# COMMAND ----------
# MAGIC %md

# K-Drama Dataset: Seed Notebook
This notebook loads the **titles** and **episodes** CSVs, creates Delta tables (optional), and demonstrates joins & simple analytics.

**Files**
- `/Workspace/Files/netflix_korean_tv_dramas_titles_enhanced.csv` (or upload to DBFS and update the paths)
- `/Workspace/Files/netflix_korean_tv_dramas_episodes.csv`
- `/Workspace/Files/netflix_korean_tv_dramas_data_dictionary.csv`

> Adjust the `base_path` for where you upload the CSVs (e.g., to `/dbfs/FileStore/kdramas/`).
# COMMAND ----------

# Paths (edit if you uploaded elsewhere)
base_path = "file:/Workspace/Files"  # for Workspace Files UI
titles_path = f"{base_path}/netflix_korean_tv_dramas_titles_enhanced.csv"
episodes_path = f"{base_path}/netflix_korean_tv_dramas_episodes.csv"
dict_path = f"{base_path}/netflix_korean_tv_dramas_data_dictionary.csv"

print(titles_path)
print(episodes_path)
# COMMAND ----------

from pyspark.sql import functions as F

titles_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(titles_path))

episodes_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(episodes_path))

display(titles_df.limit(20))
display(episodes_df.limit(20))
# COMMAND ----------
# MAGIC %md

## (Optional) Save as Delta tables
Uncomment to materialize as managed Delta tables in your current catalog & schema (requires Unity Catalog permissions).
# COMMAND ----------

# spark.sql("USE CATALOG main")
# spark.sql("USE SCHEMA default")

# titles_df.write.mode("overwrite").saveAsTable("kdrama_titles")
# episodes_df.write.mode("overwrite").saveAsTable("kdrama_episodes")

# display(spark.table("kdrama_titles").limit(5))
# display(spark.table("kdrama_episodes").limit(5))
# COMMAND ----------
# MAGIC %md

## Join examples
# COMMAND ----------

joined = (episodes_df.alias("e")
    .join(titles_df.alias("t"), F.col("e.show_id") == F.col("t.show_id"), "inner")
)

display(joined.select(
    "e.show_id","t.title","e.season_number","e.episode_number","e.episode_id",
    "t.maturity_rating","t.subrating_violence","t.subrating_language","t.subrating_sex"
).orderBy("t.title","season_number","episode_number").limit(100))
# COMMAND ----------
# MAGIC %md

## Simple analytics
# COMMAND ----------

# Avg runtime by maturity rating
display(
    titles_df.groupBy("maturity_rating").agg(F.round(F.avg("per_episode_runtime_min"),1).alias("avg_runtime_min"))
)

# Episodes per show
display(
    episodes_df.groupBy("show_id").agg(F.count("*").alias("ep_count")).orderBy(F.desc("ep_count"))
)

# Availability pivots (training flags)
avail_cols = [c for c in titles_df.columns if c.startswith("available_")]
avail_exprs = [F.sum(F.col(c).cast("int")).alias(c+"_count") for c in avail_cols]
display(titles_df.agg(*avail_exprs))
# COMMAND ----------
# MAGIC %md

## Spark SQL temp views
# COMMAND ----------

titles_df.createOrReplaceTempView("titles_v")
episodes_df.createOrReplaceTempView("episodes_v")

display(spark.sql("""
SELECT t.title, t.maturity_rating, t.subrating_violence, t.subrating_language, t.subrating_sex,
       SUM(CASE WHEN t.available_us THEN 1 ELSE 0 END) OVER () AS titles_available_us,
       COUNT(*) OVER () AS total_titles
FROM titles_v t
ORDER BY t.title
LIMIT 100
"""))