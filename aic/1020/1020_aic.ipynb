# Databricks notebook source
# MAGIC %md
# MAGIC http://jira.lge.com/issue/browse/HEDATAPLFM-1020?attachmentSortBy=dateTime&attachmentOrder=asc

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sandbox.t_weetv.po_activation_mart
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (date_ym)
# MAGIC AS
# MAGIC WITH T101 AS (
# MAGIC     /* KMS 관련 테이블 */
# MAGIC     SELECT 
# MAGIC       date_ym,
# MAGIC       mac,
# MAGIC       try_cast(po_date AS DATE) AS po_date,
# MAGIC       po_country_code,
# MAGIC       po_type,
# MAGIC       po_board_maker,
# MAGIC       po_odm,
# MAGIC       po_brand,
# MAGIC       po_country,
# MAGIC       po_year,
# MAGIC       po_platform
# MAGIC     FROM kic_data_ods.kms.kms_wee_tv
# MAGIC ),
# MAGIC T102 AS (
# MAGIC     /* ACTIVATION: MAC별 최초 */
# MAGIC     SELECT
# MAGIC         s.mac_addr,
# MAGIC         s.activ_country_code,
# MAGIC         s.activ_date,
# MAGIC         s.activ_platform_code,
# MAGIC         s.activ_product_code,
# MAGIC         s.activ_sales_model
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             T102.mac_addr,
# MAGIC             T102.Cntry_CODE   AS activ_country_code,
# MAGIC             DATE(T102.crt_date) AS activ_date,
# MAGIC             T102.Platform_code AS activ_platform_code,
# MAGIC             T102.Product_CODE  AS activ_product_code,
# MAGIC             T102.Sales_Model   AS activ_sales_model,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY T102.mac_addr
# MAGIC                 ORDER BY T102.crt_date ASC, T102.last_chg_date ASC, T102.he_etl_dt ASC
# MAGIC             ) AS rn
# MAGIC         FROM aic_data_ods.tlamp.activation_date T102
# MAGIC         WHERE EXISTS (
# MAGIC             SELECT 1
# MAGIC             FROM T101
# MAGIC             WHERE T101.mac = T102.mac_addr
# MAGIC         )
# MAGIC     ) s
# MAGIC     WHERE s.rn = 1
# MAGIC ),
# MAGIC T103 AS (
# MAGIC     /* 앱 브랜드: MAC별 최초 */
# MAGIC     SELECT
# MAGIC         s.mac_addr,
# MAGIC         s.brand_name
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             T103.mac_addr,
# MAGIC             T103.brand_name,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY T103.mac_addr
# MAGIC                 ORDER BY T103.src_file_date ASC, T103.he_etl_dt ASC
# MAGIC             ) AS rn
# MAGIC         FROM aic_data_ods.tlamp.apps_device_brand T103
# MAGIC         WHERE EXISTS (
# MAGIC             SELECT 1
# MAGIC             FROM T101
# MAGIC             WHERE T101.mac = T103.mac_addr
# MAGIC         )
# MAGIC     ) s
# MAGIC     WHERE s.rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC     -- 파티션 키
# MAGIC     T101.date_ym,
# MAGIC
# MAGIC     -- PO 측 주요 컬럼
# MAGIC     T101.mac,
# MAGIC     T101.po_date,
# MAGIC     T101.po_country_code,
# MAGIC     T101.po_type,
# MAGIC     T101.po_board_maker,
# MAGIC     T101.po_odm,
# MAGIC     T101.po_brand,
# MAGIC     T101.po_country,
# MAGIC     T101.po_year,
# MAGIC     T101.po_platform,
# MAGIC
# MAGIC     -- ACTIVATION 측
# MAGIC     T102.activ_date,
# MAGIC     T102.activ_country_code,
# MAGIC     T102.activ_platform_code,
# MAGIC     T102.activ_product_code,
# MAGIC     T102.activ_sales_model,
# MAGIC
# MAGIC     -- BRAND 파싱
# MAGIC     SPLIT_PART(T103.brand_name, '_', 1)   AS activ_board_maker,
# MAGIC     SPLIT_PART(T103.brand_name, '_', 2)   AS activ_odm,
# MAGIC     SPLIT_PART(T103.brand_name, '_', 3)   AS activ_brand,
# MAGIC
# MAGIC     -- 비교 지표
# MAGIC     CASE
# MAGIC         WHEN T101.po_country_code IS NULL THEN 'po_country_code_is_null'
# MAGIC         WHEN T102.activ_country_code IS NULL THEN 'not_yep_activated'
# MAGIC         WHEN T101.po_country_code <> T102.activ_country_code THEN 'N'
# MAGIC         ELSE 'Y'
# MAGIC     END                                      AS is_same_country,
# MAGIC     DATE_DIFF(T102.activ_date, T101.po_date) AS day_diff,
# MAGIC     
# MAGIC     current_timestamp() as he_etl_dt
# MAGIC FROM T101
# MAGIC LEFT JOIN T102
# MAGIC   ON T102.mac_addr = T101.mac
# MAGIC LEFT JOIN T103
# MAGIC   ON T103.mac_addr = T101.mac
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sandbox.z_yeswook_kim.v_rbt_x_user_number AS
# MAGIC WITH user_num_dimension AS (
# MAGIC   SELECT X_User_Number AS origin_X_User_Number, X_User_Number_hashed AS X_User_Number
# MAGIC   FROM aic_data_private.tlamp.rbt_x_user_number_webos22
# MAGIC   UNION ALL
# MAGIC   SELECT X_User_Number, X_User_Number_hashed
# MAGIC   FROM aic_data_private.tlamp.rbt_x_user_number_webos23
# MAGIC   UNION ALL
# MAGIC   SELECT X_User_Number, X_User_Number_hashed
# MAGIC   FROM aic_data_private.tlamp.rbt_x_user_number_webos24
# MAGIC   UNION ALL
# MAGIC   SELECT X_User_Number, X_User_Number_hashed
# MAGIC   FROM aic_data_private.tlamp.rbt_x_user_number_webos25
# MAGIC   UNION ALL
# MAGIC   SELECT X_User_Number, X_User_Number_hashed
# MAGIC   FROM aic_data_private.tlamp.rbt_x_user_number_webos60
# MAGIC ),
# MAGIC distinct_user_num_dimension AS (
# MAGIC   SELECT DISTINCT origin_X_User_Number, X_User_Number
# MAGIC   FROM user_num_dimension
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM distinct_user_num_dimension
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sandbox.z_yeswook_kim.v_rbt_mac_addr AS
# MAGIC WITH user_num_dimension AS (
# MAGIC   SELECT mac_addr AS origin_mac_addr, mac_addr_hashed AS mac_addr
# MAGIC   FROM aic_data_private.tlamp.rbt_mac_addr_webos22
# MAGIC   UNION ALL
# MAGIC   SELECT mac_addr, mac_addr_hashed
# MAGIC   FROM aic_data_private.tlamp.rbt_mac_addr_webos23
# MAGIC   UNION ALL
# MAGIC   SELECT mac_addr, mac_addr_hashed
# MAGIC   FROM aic_data_private.tlamp.rbt_mac_addr_webos24
# MAGIC   UNION ALL
# MAGIC   SELECT mac_addr, mac_addr_hashed
# MAGIC   FROM aic_data_private.tlamp.rbt_mac_addr_webos25
# MAGIC   UNION ALL
# MAGIC   SELECT mac_addr, mac_addr_hashed
# MAGIC   FROM aic_data_private.tlamp.rbt_mac_addr_webos60
# MAGIC   /* activation에만 있는 mac도 확보*/
# MAGIC   UNION ALL
# MAGIC   SELECT mac_addr, mac_addr_hashed
# MAGIC   FROM sandbox.z_yeswook_kim.rbt_mac_addr_activation_date  
# MAGIC ),
# MAGIC distinct_user_num_dimension AS (
# MAGIC   SELECT DISTINCT origin_mac_addr, mac_addr
# MAGIC   FROM user_num_dimension
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM distinct_user_num_dimension
# MAGIC

# COMMAND ----------

# DBTITLE 1,activtiaon rbt

salt = dbutils.secrets.get("admin", "salt")

sdf = spark.sql(f"""
    CREATE OR REPLACE TABLE sandbox.z_yeswook_kim.rbt_mac_addr_activation_date AS
    SELECT
        distinct 
        CASE
            WHEN mac_addr IS NULL OR mac_addr = '' THEN mac_addr
            ELSE sha2(CONCAT(mac_addr, '{salt}'), 256)
        END AS mac_addr_hashed,
        mac_addr,
        current_timestamp() AS he_etl_dt
    FROM aic_data_private.tlamp.activation_date
""")

display(spark.table("sandbox.z_yeswook_kim.rbt_mac_addr_activation_date"))

# COMMAND ----------

# DBTITLE 1,1020_AIC
# MAGIC %sql
# MAGIC select 
# MAGIC   b.origin_mac_addr
# MAGIC   , a.*
# MAGIC   -- count(1)
# MAGIC from sandbox.t_weetv.po_activation_mart a
# MAGIC left join sandbox.z_yeswook_kim.v_rbt_mac_addr b 
# MAGIC   on a.mac = b.mac_addr
# MAGIC where 1=1
# MAGIC   and po_type like 'WEE 2.0%'
# MAGIC   and (po_board_maker != activ_board_maker
# MAGIC     or po_odm != activ_odm)

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   mac, count(1)
# MAGIC from sandbox.t_weetv.po_activation_mart
# MAGIC group by mac
# MAGIC having count(1) >= 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   b.origin_mac_addr,
# MAGIC   count(1) as cnt
# MAGIC from sandbox.t_weetv.po_activation_mart a
# MAGIC left join sandbox.z_yeswook_kim.v_rbt_mac_addr b 
# MAGIC   on a.mac = b.mac_addr
# MAGIC where 1=1
# MAGIC   and po_type like 'WEE 2.0%'
# MAGIC   and (po_board_maker != activ_board_maker
# MAGIC     or po_odm != activ_odm)
# MAGIC group by b.origin_mac_addr
# MAGIC having count(1) >= 2

# COMMAND ----------

# MAGIC %sql
# MAGIC