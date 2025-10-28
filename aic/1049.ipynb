# Databricks notebook source
# MAGIC %md
# MAGIC ## Overview
# MAGIC - 0. dim_date_ym                                        : dimension data (master) 
# MAGIC - 1. mac_addr  first_act_date                          : raw data
# MAGIC - 2. joinA : dim_date_ym  mac_addr   first_act_date
# MAGIC -                 2024.01     A           2024.01
# MAGIC -                 2024.02     A           2024.01
# MAGIC -                   ...
# MAGIC -                 2025.03     A           2024.01
# MAGIC - 3. mac_addr  date_ym  -> last(dpv)                   : pivot data
# MAGIC -         A      2024.01      8.0
# MAGIC -         A      2024.05      9.0
# MAGIC - 4. joinB : joinA + pivot_data
# MAGIC -     dim_date_ym  mac_addr  first_act_date  mac_addr  date_ym  last(dpv)   rn
# MAGIC -        2024.01     A          2024.01         A        2024.01     8.0      
# MAGIC -        2024.02     A          2024.01         A        2024.01     8.0       2 (조건2, orderBy mac_addr/dim_date/date_ym desc)
# MAGIC -         ...        A          2024.01         A        2024.01     8.0
# MAGIC -        2025.03     A          2024.01         A        2024.01     8.0
# MAGIC - 
# MAGIC -        2024.01     A          2024.01         A        2024.02     9.0       - (조건1, dim_date_ym < date_ym)
# MAGIC -        2024.02     A          2024.01         A        2024.02     9.0   
# MAGIC -         ...        A          2024.01         A        2024.02     9.0
# MAGIC -        2025.03     A          2024.01         A        2024.02     9.0
# MAGIC - 5. joinB 필터 적용
# MAGIC       - 조건1) dim_date_ym >= date_ym
# MAGIC       - 조건2) rn = 1, (order by dim_date_ym, date_ym desc) as rn
# MAGIC - 6. 중간 날짜 로그 없는 경우, 이전 날짜 값으로 치환 
# MAGIC - 7. joinC : joinB(mac_addr) + 컬럼 추가
# MAGIC       - 테이블1) use_mac_user_master : mac_addr, country_code, platform_code, sales_model_code 
# MAGIC       - 테이블2) 인치 정보
# MAGIC       - 하드코딩) platform_version
# MAGIC - 8. 검토 :
# MAGIC       - 조건1) inch가 잘 조인되었는지 : sales_model_code별 inch가 1개 조회되는지 (where platform_version  not like 'WEE%')
# MAGIC - 9. 집계 : count_ud
# MAGIC -     dim_date_ym  country_code  platform_version  platform_code  sales_model_code  inch  dpv  count_ud
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## -1. 파라미터 입력

# COMMAND ----------

p_pv = str(dbutils.widgets.get("p_pv")) # platform version
print(p_pv, type(p_pv))

if p_pv == 'webOSTV 22' :
    p_normal_table = 'normal_log_webos22'
elif p_pv == 'webOSTV 23' :
    p_normal_table = 'normal_log_webos23'
elif p_pv == 'webOSTV 24' :
    p_normal_table = 'normal_log_webos24'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. 날짜범위 구하기 : date_ym dimension 만들기 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# Set schema/data
schema = StructType([
    StructField('dim_date_ym', StringType(), True)
])
data = [('2025-08',),]
# Create DataFrame
df_dim = spark.createDataFrame(data, schema)
df_dim.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 타겟 mac 구하기 : KR mac_addr 별 min(activation_date) 구하기

# COMMAND ----------

# 1. 2,573,502
df_raw = spark.sql(f'''
    select   mac_addr, min(first_activation_date) as first_activation_date, date_format(min(first_activation_date), 'yyyy-MM') as first_act_date_ym
    from     aic_data_mart.master_tables.use_mac_user_master
    group by mac_addr
''')
df_raw.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. [joinA] 타겟 mac_addr별 조사할 date_ym 범위 구하기

# COMMAND ----------

# 2. joinA : dim_date_ym  device_id   first_act_date
from pyspark.sql.functions import col

df_join_a = df_raw.crossJoin(df_dim) # cross join
df_join_a = df_join_a.where("first_act_date_ym <= dim_date_ym")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 검증

# COMMAND ----------

df_join_a.where("mac_addr = '001145c0f035011b10b1f6238b97193d88949f48b450d8923ec81f5302d25881'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. [mac_dpv_1m] 타겟 mac_addr에 대한 월 dpv 값 구하기 
# MAGIC > device_id date_ym -> last(dpv) : pivot data

# COMMAND ----------

df_dpv = spark.sql(f'''
    select mac_addr, date_ym, RELEASE_NUMBER as dpv
    from   (
        SELECT mac_addr, date_ym, row_number() over(partition by mac_addr, date_ym order by log_create_time desc) as rn, RELEASE_NUMBER
        FROM   aic_data_ods.tlamp_private.{p_normal_table}
        WHERE  1=1
           AND context_name = 'tvpowerd'
           AND message_id = 'NL_POWER_STATE'
           AND date_ym = '2025-08') t
    WHERE 1=1
      AND rn = 1
''')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 검증

# COMMAND ----------

df_dpv.count()

# COMMAND ----------

df_dpv.select('mac_addr', 'date_ym').distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. [joinB] joinA + mac_dpv_1m

# COMMAND ----------

df_join_b = df_join_a.join(df_dpv, on='mac_addr', how='left_outer')

# COMMAND ----------

df_join_b.orderBy('mac_addr', 'dim_date_ym', 'date_ym').limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. [joinB] 필터

# COMMAND ----------

# MAGIC %md
# MAGIC #### 조건 1

# COMMAND ----------

# 조건 1 
df_join_b1 = df_join_b.where("(date_ym is not null and dim_date_ym >= date_ym) or (date_ym is null)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 조건 2

# COMMAND ----------

# 조건 2
from pyspark.sql.functions import row_number, when
from pyspark.sql.window import Window 

# null 여부를 flag로 지정 (null이면 1, 아니면 0)
df_join_b1 = df_join_b1.withColumn("null_flag", when(col("dpv").isNull(), 1).otherwise(0))

# 윈도우 정의
window_spec = Window.partitionBy("mac_addr", "dim_date_ym")\
                    .orderBy("null_flag", df_join_b1['date_ym'].desc())

# row_number 추가
df_join_b1 = df_join_b1.withColumn("rn", row_number().over(window_spec))

# rn = 1인 데이터만 필터링
df_join_b2 = df_join_b1.filter(df_join_b1["rn"] == 1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 검증

# COMMAND ----------

# 검증
df_join_b2.orderBy('mac_addr', 'dim_date_ym', 'date_ym').limit(100).display()

# COMMAND ----------

from pyspark.sql.functions import count, max, min

# mac_addr별 count를 계산한 뒤, 그 중 최대값 추출
df_counts = df_join_b2.groupBy("mac_addr").agg(count("dim_date_ym").alias("cnt"))
df_counts.agg(max("cnt")).show()

from pyspark.sql.functions import count, max

# mac_addr별 count를 계산한 뒤, 그 중 최소값 추출
df_counts = df_join_b2.groupBy("mac_addr").agg(count("dim_date_ym").alias("cnt"))
df_counts.agg(min("cnt")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 중간 날짜에 로그가 없어서 null인 경우, 이전 날짜의 값으로 채우기

# COMMAND ----------

from pyspark.sql.functions import to_date, last

# Window 정의 (device별 날짜 정렬)
w = Window.partitionBy("mac_addr").orderBy("dim_date_ym").rowsBetween(Window.unboundedPreceding, 0)

# 누적된 value_str (앞에서부터 채우기 - forward fill)
df_join_bf = df_join_b2.withColumn("filled_dpv", last("dpv", ignorenulls=True).over(w))
df_join_bf.where("dpv != filled_dpv").limit(10).display()


# COMMAND ----------

# 검증
df_join_bf.orderBy('mac_addr', 'dim_date_ym', 'date_ym').limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 컬럼 추가 조인

# COMMAND ----------

# MAGIC %md
# MAGIC       - 테이블1) use_mac_user_master : mac_addr, country_code, platform_code, sales_model_code 
# MAGIC       - 테이블2) 인치 정보
# MAGIC       - 하드코딩) platform_version

# COMMAND ----------

df_dim_mac = spark.sql(f''' 
    select distinct mac_addr
           , country_code 
           , platform_code 
           , sales_model_code 
    from   aic_data_mart.master_tables.use_mac_user_master
''')
df_join_c = df_join_bf.join(df_dim_mac, on='mac_addr', how='left_outer')
df_join_c.limit(10).display()

# COMMAND ----------

df_tv_model = spark.sql(f'''
    -- 인치 정보
    select sales_model_code, last(inch) as inch
    from   aic_data_dimension.common_tv.tv_model
    where  use_yn = 'Y'
    group by sales_model_code
''')
df_join_c2 = df_join_c.join(df_tv_model, on='sales_model_code', how='left_outer')
df_join_c2.limit(10).display()


# COMMAND ----------

from pyspark.sql.functions import lit
df_join_cf = df_join_c2.withColumn("platform_version", lit(p_pv))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7-1. 저장 to delta

# COMMAND ----------

df_join_cf\
    .write.mode('overwrite')\
    .saveAsTable(f"sandbox.z_eunmi1_ko.temp_{p_normal_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 집계 

# COMMAND ----------

region = 'AIC'
df_result = spark.sql(f'''
    -- filled_value null 추가 보완 : '24/09 이전 로그 탐색
    with extension_dpv_webos22 as (
    
    -- 마지막 dpv 가져오기 : 
    select mac_addr, X_Device_SDK_VERSION as dpv
    from   (
        select mac_addr, row_number() over(partition by mac_addr order by log_create_time desc) as rn, X_Device_SDK_VERSION
        from   aic_data_ods.tlamp_private.normal_log_webos22
        where  date_ym >= '2022-03'
            AND  date_ym < '2025-08'
            AND  context_name = 'tvpowerd'
            AND  message_id   = 'NL_POWER_STATE'
            AND  mac_addr in (
            select distinct mac_addr
            from   sandbox.z_eunmi1_ko.temp_normal_log_webos22
            where  filled_dpv is null 
        ) 
    ) 
    where rn = 1
    ), extension_dpv_webos23 as (
    
    -- 마지막 dpv 가져오기 : 
    select mac_addr, X_Device_SDK_VERSION as dpv
    from   (
        select mac_addr, row_number() over(partition by mac_addr order by log_create_time desc) as rn, X_Device_SDK_VERSION
        from   aic_data_ods.tlamp_private.normal_log_webos23
        where  date_ym >= '2023-03'
            AND  date_ym < '2025-08'
            AND  context_name = 'tvpowerd'
            AND  message_id   = 'NL_POWER_STATE'
            AND  mac_addr in (
            select distinct mac_addr
            from   sandbox.z_eunmi1_ko.temp_normal_log_webos23
            where  filled_dpv is null 
        ) 
    ) 
    where rn = 1
    ), extension_dpv_webos24 as (
    
    -- 마지막 dpv 가져오기 : 
    select mac_addr, X_Device_SDK_VERSION as dpv
    from   (
        select mac_addr, row_number() over(partition by mac_addr order by log_create_time desc) as rn, X_Device_SDK_VERSION
        from   aic_data_ods.tlamp_private.normal_log_webos24
        where  date_ym >= '2024-03'
            AND  date_ym < '2025-08'
            AND  context_name = 'tvpowerd'
            AND  message_id   = 'NL_POWER_STATE'
            AND  mac_addr in (
            select distinct mac_addr
            from   sandbox.z_eunmi1_ko.temp_normal_log_webos24
            where  filled_dpv is null 
        ) 
    ) 
    where rn = 1
    ), mart_final_webos22 as (
    select mart.mac_addr, mart.dim_date_ym, mart.country_code, mart.platform_version, mart.platform_code, mart.sales_model_code, mart.inch
            , (case when mart.filled_dpv is null then ext.dpv else mart.filled_dpv end) as final_dpv
    from   sandbox.z_eunmi1_ko.temp_normal_log_webos22 as mart
    left join extension_dpv_webos22 as ext using (mac_addr)
    ), mart_final_webos23 as (
    select mart.mac_addr, mart.dim_date_ym, mart.country_code, mart.platform_version, mart.platform_code, mart.sales_model_code, mart.inch
            , (case when mart.filled_dpv is null then ext.dpv else mart.filled_dpv end) as final_dpv
    from   sandbox.z_eunmi1_ko.temp_normal_log_webos23 as mart
    left join extension_dpv_webos23 as ext using (mac_addr)
    ), mart_final_webos24 as (
    select mart.mac_addr, mart.dim_date_ym, mart.country_code, mart.platform_version, mart.platform_code, mart.sales_model_code, mart.inch
            , (case when mart.filled_dpv is null then ext.dpv else mart.filled_dpv end) as final_dpv
    from   sandbox.z_eunmi1_ko.temp_normal_log_webos24 as mart
    left join extension_dpv_webos24 as ext using (mac_addr)
    )

    -- 지표 결과 2 : 16,722 return
    select tr.*, tc.region
    from  (
    select dim_date_ym, country_code, platform_version, platform_code, sales_model_code, inch, final_dpv, count(distinct mac_addr) as cnt_ud
    from   mart_final_webos22
    where  final_dpv is not null
    group by dim_date_ym, country_code, platform_version, platform_code, sales_model_code, inch, final_dpv

    union all 
    select dim_date_ym, country_code, platform_version, platform_code, sales_model_code, inch, final_dpv, count(distinct mac_addr) as cnt_ud
    from   mart_final_webos23
    where  final_dpv is not null
    group by dim_date_ym, country_code, platform_version, platform_code, sales_model_code, inch, final_dpv

    union all 
    select dim_date_ym, country_code, platform_version, platform_code, sales_model_code, inch, final_dpv, count(distinct mac_addr) as cnt_ud
    from   mart_final_webos24
    where  final_dpv is not null
    group by dim_date_ym, country_code, platform_version, platform_code, sales_model_code, inch, final_dpv
    ) tr
    left join (
    select distinct country_code AS cc, region
    from aic_data_dimension.common.country_code
    where region = '{region}'
    ) as tc ON lower(tr.country_code) = lower(tc.cc)
    where tc.cc is not null and len(replace(final_dpv, ' ', '')) > 1
''')

# COMMAND ----------

df_result.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(f's3://s3-lge-he-inbound-aic-dev/HEDS/HEDS-1049/{p_pv}')

# COMMAND ----------

