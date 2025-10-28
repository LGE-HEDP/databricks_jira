# Databricks notebook source
http://jira.lge.com/issue/browse/HEDATAPLFM-1094?attachmentSortBy=dateTime&attachmentOrder=asc

# COMMAND ----------

!pip install openpyxl

# COMMAND ----------

import pandas as pd
# Excel 파일 경로
file_path = '/Volumes/sandbox/z_yeswook_kim/v_yeswook_kim/heds/MAC_98UT9_VRR_이슈.xlsx'

# Pandas로 Excel 읽기
pdf = pd.read_excel(file_path, engine='openpyxl')

# Spark DataFrame으로 변환
sdf = spark.createDataFrame(pdf)


# COMMAND ----------

from pyspark.sql.functions import sha2, expr, current_timestamp, col, when, concat, lit, date_format, split, size, substring, to_timestamp

tv_salt = dbutils.secrets.get("admin", "salt")

df_public = sdf.withColumn("mac_addr", when(col("MAC Address").isNull() | (col("MAC Address") == ''), col("MAC Address")).otherwise(sha2(concat(col("MAC Address"), lit(tv_salt)), 256)))

# COMMAND ----------

display(df_public)

# COMMAND ----------


# 필요한 컬럼 선택 및 저장
df_public.select(
    col('`No.`').alias("no"),
    col('`MAC Address`').alias("mac_origin"),
    col("mac_addr")
).write.format("delta") \
 .mode("overwrite") \
 .option("overwriteSchema", "true") \
 .saveAsTable("sandbox.z_yeswook_kim.heds1094")


# COMMAND ----------

qr = f"""
with tr_mac as (
  select 
    heds1094.mac_addr as mac_addr, 
    mac_origin as real_mac
  from sandbox.z_yeswook_kim.heds1094 heds1094
), actv as (
  select 
    mac_addr,
    min(crt_date) as min_crt_date,
    max(last_chg_date) as max_last_chg_date
  from aic_data_ods.tlamp.activation_date
  where 1=1
  and mac_addr in (select mac_addr from tr_mac)
  group by mac_addr
), normal as (
  select 
    mac_addr,
    max(log_create_time) as max_log_create_time,
    max(accum_run_time) as max_accum_run_time
  from (
    select 
      mac_addr,
      log_create_time,
      accum_run_time
    from aic_data_ods.tlamp.normal_log_webos24
    where 1=1
    and mac_addr in (select mac_addr from tr_mac)
    
    union all

    select 
      mac_addr,
      log_create_time,
      accum_run_time
    from aic_data_ods.tlamp.normal_log_webos25
    where 1=1
    and mac_addr in (select mac_addr from tr_mac)

    )
  group by mac_addr
)
select 
  tr_mac.real_mac,
  org.*,
  actv.min_crt_date,
  actv.max_last_chg_date,
  normal.max_log_create_time,
  normal.max_accum_run_time
from sandbox.z_yeswook_kim.heds1094 org
left outer join tr_mac
on org.mac_addr=tr_mac.mac_addr
left outer join actv
on org.mac_addr=actv.mac_addr
left outer join normal
on org.mac_addr=normal.mac_addr
"""

df= spark.sql(qr)

df.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save('s3://s3-lge-he-sandbox-aic/z_yeswook_kim/volume/v_yeswook_kim/heds/HEDS-1094-aic')
