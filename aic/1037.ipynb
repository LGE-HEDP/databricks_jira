# Databricks notebook source
df = spark.sql('''
               -- select count(1) -- 7,513,673
    SELECT
        X_device_platform as `platform_code`,  -- 필수 열
        X_device_product as `platform_version`,-- 필수 열
        X_Device_Country as `country_code`,    -- 필수/조건 열
        log_create_time,                       -- 필수/조건 열
        -- X_Device_SDK_VERSION as `dpv`,
        mac_addr,               -- 필수 열  
        x_device_sales_model as `sales_model_code`, -- 필수 열
        context_name,                          -- 필수/조건 열  
        message_id,                            -- 필수/조건 열
        normal_log:terrestrial_analog_tv,
        normal_log:terrestrial_digital_tv,
        normal_log:cable_analog_tv,
        normal_log:cable_digital_tv,
        normal_log:terrestrial_digital_tv_uhd,
    normal_log:cable_digital_tv_uhd
    FROM aic_data_ods.tlamp.normal_log_webos23
    WHERE 1=1
        AND X_device_country = 'US'
        AND context_name = 'BROADCAST'
        AND message_id = 'NL_BROADCAST_CHANNEL_COUNT'
        AND date_ym >= '2024-08'
        AND date_ym < '2025-06'
               ''')

# COMMAND ----------

df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("s3://s3-lge-he-inbound-aic-dev/HEDS/HEDS-1037/NL_BROADCAST_CHANNEL_COUNT/")

# COMMAND ----------

df2 = spark.sql('''

-- select count(1) -- 1,421,439
SELECT
    X_device_platform as `platform_code`,  -- 필수 열
    X_device_product as `platform_version`,-- 필수 열
    X_Device_Country as `country_code`,    -- 필수/조건 열
    log_create_time,                       -- 필수/조건 열
    -- X_Device_SDK_VERSION as `dpv`,
    mac_addr,               -- 필수 열  
    x_device_sales_model as `sales_model_code`, -- 필수 열
    context_name,                          -- 필수/조건 열  
    message_id,                            -- 필수/조건 열
    normal_log:atv, normal_log:dtv, normal_log:catv, normal_log:cadtv, normal_log:dtv_uhd, normal_log:cadtv_uhd
FROM aic_data_ods.tlamp.normal_log_webos23
WHERE 1=1
    AND X_device_country = 'US'
    AND context_name = 'BROADCAST'
    AND message_id = 'NL_BROADCAST_WATCH_CHANNELSOURCE'
    AND date_ym >= '2024-08'
    AND date_ym < '2025-06'
''')

# COMMAND ----------

df2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("s3://s3-lge-he-inbound-aic-dev/HEDS/HEDS-1037/NL_BROADCAST_WATCH_CHANNELSOURCE/")

# COMMAND ----------

