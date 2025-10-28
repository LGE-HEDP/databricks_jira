# Databricks notebook source
# MAGIC %md
# MAGIC http://jira.lge.com/issue/browse/HEDATAPLFM-1040?attachmentSortBy=dateTime&attachmentOrder=asc
# MAGIC

# COMMAND ----------

df = spark.read.csv(
    "/Volumes/sandbox/z_najin_lee/v_najin_lee/lgch_rec_segment_0925.csv",
    header=True,
    inferSchema=True,
)

df.show(10)

# COMMAND ----------

from pyspark.sql.functions import (
    sha2,
    concat_ws,
    current_timestamp,
    when,
    col,
    lit,
    sha2,
    concat,
)


he_etl_dt = current_timestamp()
tv_salt = dbutils.secrets.get("admin", "salt")


# df_private 컬럼 추가
df = df.withColumn(
    "mac_addr",
    when(col("mac").isNull() | (col("mac") == ""), col("mac")).otherwise(
        sha2(concat(col("mac"), lit(tv_salt)), 256)
    ),
)

df = df.withColumn("he_etl_dt", he_etl_dt)

display(df)

# COMMAND ----------

df = df.drop("mac")
display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("sandbox.z_najin_lee.lgch_rec_segment_0925")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from sandbox.z_najin_lee.lgch_rec_segment_0925
# MAGIC limit 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct mac
# MAGIC from sandbox.z_najin_lee.lgch_rec_segment_0925
# MAGIC limit 20;