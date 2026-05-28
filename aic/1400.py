# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## JIRA 1400 - PINE25(Re:New25) 업데이트 비중 월별 추이 데이터 추출
# MAGIC
# MAGIC **요구사항:**
# MAGIC - 데이터 소스 : 데이터 수집에 동의한 디바이스
# MAGIC - 기간 : 2025-08 ~ 2026-04
# MAGIC - 국가 : 글로벌
# MAGIC - 플랫폼 : webOS22 / webOS23 / webOS24
# MAGIC - 기타 : 플랫폼 버전, DPV 버전, 제품명, 인치, 지역/국가, UD
# MAGIC
# MAGIC **파이프라인 (1397 기반):**
# MAGIC 1. dim_date_ym 생성 (2025-08 ~ 2026-04)
# MAGIC 2. mac_addr별 first_activation_date 조회
# MAGIC 3. cross join → 날짜 범위 확장
# MAGIC 4. 플랫폼별 DPV 조회
# MAGIC 5. join + 필터 (조건1: dim_date_ym >= date_ym, 조건2: rn=1)
# MAGIC 6. forward fill (null dpv → 이전값)
# MAGIC 7. 마스터/인치 조인 + platform_version 컬럼 추가
# MAGIC 8. 글로벌 집계 (region 필터 없음)
# MAGIC 9. S3 저장

# COMMAND ----------

# DBTITLE 1,파라미터 설정
# 파라미터 설정
list_pv = ['webOSTV 22', 'webOSTV 23', 'webOSTV 24']

# 플랫폼별 테이블 매핑
pv_table_map = {
    'webOSTV 22': 'normal_log_webos22',
    'webOSTV 23': 'normal_log_webos23',
    'webOSTV 24': 'normal_log_webos24',
}

print(f"처리할 플랫폼: {list_pv}")

# COMMAND ----------

# DBTITLE 1,0. dim_date_ym 생성 (2025-08 ~ 2026-04)
# 0. dim_date_ym 생성 : 2025-08 ~ 2026-04
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([StructField('dim_date_ym', StringType(), True)])
data = [
    ('2025-08',), ('2025-09',), ('2025-10',), ('2025-11',), ('2025-12',),
    ('2026-01',), ('2026-02',), ('2026-03',), ('2026-04',),
]

df_dim = spark.createDataFrame(data, schema)
df_dim.display()

# COMMAND ----------

# DBTITLE 1,날짜 리스트 추출
import builtins
builtins_min = builtins.min

list_target_date_ym = [row.dim_date_ym for row in df_dim.select('dim_date_ym').collect()]
min_date_ym = builtins_min(list_target_date_ym)
print(f"대상 기간: {list_target_date_ym}")
print(f"최소 날짜: {min_date_ym}")

# COMMAND ----------

# DBTITLE 1,1. 타겟 mac_addr 조회 (first_activation_date)
# 1. 타겟 mac 구하기 : mac_addr별 min(activation_date)
df_raw = spark.sql('''
    SELECT mac_addr
           , min(first_activation_date) AS first_activation_date
           , date_format(min(first_activation_date), 'yyyy-MM') AS first_act_date_ym
    FROM   aic_data_mart.master_tables.use_mac_user_master
    GROUP BY mac_addr
''')
print(f"전체 mac_addr 수: {df_raw.count():,}")
df_raw.limit(5).display()

# COMMAND ----------

# DBTITLE 1,2. joinA - cross join으로 날짜 범위 확장
# 2. joinA : mac_addr별 조사할 date_ym 범위 구하기
from pyspark.sql.functions import col

df_join_a = df_raw.crossJoin(df_dim)
df_join_a = df_join_a.where("first_act_date_ym <= dim_date_ym")
print(f"joinA 건수: {df_join_a.count():,}")

# COMMAND ----------

# DBTITLE 1,3~9. 플랫폼별 루프 처리 (DPV → join → fill → 집계 → 저장)
# 3~9. 플랫폼별 전체 파이프라인 처리
from pyspark.sql.functions import row_number, when, last, lit, count, col
from pyspark.sql.window import Window
from functools import reduce
from pyspark.sql import DataFrame

list_results = []

for p_pv in list_pv:
    p_normal_table = pv_table_map[p_pv]
    print(f"\n{'='*60}")
    print(f"처리 중: {p_pv} ({p_normal_table})")
    print(f"{'='*60}")
    
    # 3. DPV 조회
    df_dpv = spark.sql(f'''
        SELECT mac_addr, date_ym, RELEASE_NUMBER AS dpv
        FROM   (
            SELECT mac_addr, date_ym,
                   row_number() OVER(PARTITION BY mac_addr, date_ym ORDER BY log_create_time DESC) AS rn,
                   RELEASE_NUMBER
            FROM   aic_data_ods.tlamp_private.{p_normal_table}
            WHERE  context_name = 'tvpowerd'
              AND  message_id = 'NL_POWER_STATE'
              AND  date_ym IN ({','.join([repr(x) for x in list_target_date_ym])})
        ) t
        WHERE rn = 1
    ''')
    print(f"  DPV 건수: {df_dpv.count():,}")
    
    # 4. joinB
    df_join_b = df_join_a.join(df_dpv, on='mac_addr', how='left_outer')
    
    # 5. 필터 - 조건1: dim_date_ym >= date_ym
    df_join_b1 = df_join_b.where("(date_ym IS NOT NULL AND dim_date_ym >= date_ym) OR (date_ym IS NULL)")
    
    # 5. 필터 - 조건2: rn=1 (파티션별 최신 dpv)
    df_join_b1 = df_join_b1.withColumn("null_flag", when(col("dpv").isNull(), 1).otherwise(0))
    window_spec = Window.partitionBy("mac_addr", "dim_date_ym").orderBy("null_flag", col('date_ym').desc())
    df_join_b1 = df_join_b1.withColumn("rn", row_number().over(window_spec))
    df_join_b2 = df_join_b1.filter(col("rn") == 1)
    
    # 6. Forward fill (null dpv → 이전값)
    w = Window.partitionBy("mac_addr").orderBy("dim_date_ym").rowsBetween(Window.unboundedPreceding, 0)
    df_join_bf = df_join_b2.withColumn("filled_dpv", last("dpv", ignorenulls=True).over(w))
    
    # 7. 마스터 조인 (country_code, platform_code, sales_model_code)
    df_dim_mac = spark.sql('''
        SELECT DISTINCT mac_addr, country_code, platform_code, sales_model_code
        FROM   aic_data_mart.master_tables.use_mac_user_master
    ''')
    df_join_c = df_join_bf.join(df_dim_mac, on='mac_addr', how='left_outer')
    
    # 7. 인치 조인
    df_tv_model = spark.sql('''
        SELECT sales_model_code, last(inch) AS inch
        FROM   aic_data_dimension.common_tv.tv_model
        WHERE  use_yn = 'Y'
        GROUP BY sales_model_code
    ''')
    df_join_c2 = df_join_c.join(df_tv_model, on='sales_model_code', how='left_outer')
    
    # 7. platform_version 컬럼 추가
    df_join_cf = df_join_c2.withColumn("platform_version", lit(p_pv))
    
    # 7-1. 임시 테이블 저장
    df_join_cf.write.mode('overwrite').saveAsTable(f"sandbox.z_yeswook_kim.temp_{p_normal_table}")
    print(f"  임시 테이블 저장 완료: sandbox.z_yeswook_kim.temp_{p_normal_table}")
    
    # 8. 글로벌 집계 (region 필터 없이 전체 국가)
    df_result = spark.sql(f'''
        WITH extension_dpv AS (
            SELECT mac_addr, X_Device_SDK_VERSION AS dpv
            FROM   (
                SELECT mac_addr,
                       row_number() OVER(PARTITION BY mac_addr ORDER BY log_create_time DESC) AS rn,
                       X_Device_SDK_VERSION
                FROM   aic_data_ods.tlamp_private.{p_normal_table}
                WHERE  date_ym < '{min_date_ym}'
                  AND  context_name = 'tvpowerd'
                  AND  message_id   = 'NL_POWER_STATE'
                  AND  mac_addr IN (
                    SELECT DISTINCT mac_addr
                    FROM   sandbox.z_yeswook_kim.temp_{p_normal_table}
                    WHERE  filled_dpv IS NULL
                )
            )
            WHERE rn = 1
        ), mart_final AS (
            SELECT mart.mac_addr, mart.dim_date_ym, mart.country_code,
                   mart.platform_version, mart.platform_code, mart.sales_model_code,
                   mart.inch,
                   CASE WHEN mart.filled_dpv IS NULL THEN ext.dpv ELSE mart.filled_dpv END AS final_dpv
            FROM   sandbox.z_yeswook_kim.temp_{p_normal_table} AS mart
            LEFT JOIN extension_dpv AS ext USING (mac_addr)
        )
        SELECT tr.*, tc.region
        FROM  (
            SELECT dim_date_ym, country_code, platform_version, platform_code,
                   sales_model_code, inch, final_dpv,
                   count(DISTINCT mac_addr) AS cnt_ud
            FROM   mart_final
            WHERE  final_dpv IS NOT NULL
            GROUP BY dim_date_ym, country_code, platform_version, platform_code,
                     sales_model_code, inch, final_dpv
        ) tr
        LEFT JOIN (
            SELECT country_code AS cc, region
            FROM   aic_data_dimension.common.country_code
        ) AS tc
        ON lower(tr.country_code) = lower(tc.cc)
        WHERE len(replace(final_dpv, ' ', '')) > 1
    ''')
    
    result_cnt = df_result.count()
    print(f"  집계 결과 건수: {result_cnt:,}")
    list_results.append(df_result)

# 전체 플랫폼 결과 합치기
df_final = reduce(DataFrame.unionAll, list_results)
print(f"\n전체 결과 건수: {df_final.count():,}")

# COMMAND ----------

# DBTITLE 1,결과 확인
# 결과 확인
df_final.limit(20).display()

# COMMAND ----------

# DBTITLE 1,9. S3 저장
# 9. S3 저장
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_name = notebook_path.split("/")[-1]

# 플랫폼별 개별 저장
for p_pv in list_pv:
    p_normal_table = pv_table_map[p_pv]
    df_pv_result = df_final.filter(col('platform_version') == p_pv)
    df_pv_result.coalesce(1).write.format('com.databricks.spark.csv') \
        .mode('overwrite').option('header', 'true') \
        .save(f's3://s3-lge-he-inbound-aic-dev/HEDS/{notebook_name}/{p_pv}')
    print(f"저장 완료: s3://s3-lge-he-inbound-aic-dev/HEDS/{notebook_name}/{p_pv}")

print("\n모든 플랫폼 저장 완료!")