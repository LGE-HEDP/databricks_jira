# Databricks notebook source
# MAGIC %md
# MAGIC http://jira.lge.com/issue/browse/HEDATAPLFM-1032?attachmentSortBy=dateTime&attachmentOrder=asc
# MAGIC
# MAGIC - webOS Platform : webOS 버전 ex) webOS22
# MAGIC - TV device ID : TV를 구분할 수 있는 고유 ID
# MAGIC - spec : NL_HC_DEVICELIST 을 통해 출력 되는 spec 정보
# MAGIC - type : NL_HC_DEVICELIST 을 통해 출력 되는 type 정보
# MAGIC

# COMMAND ----------

# DBTITLE 1,추출쿼리
# MAGIC %sql
# MAGIC create table if not exists adhoc.heds.1032_cl0 as 
# MAGIC with base_tb as (
# MAGIC   select 
# MAGIC   substr(X_Device_Platform, 1, 3) as webOS_platform
# MAGIC   , mac_addr
# MAGIC   , explode(from_json(normal_log:dvc_list, 'array<struct<name:string,type:string,spec:string,id:string,label:string>>')) AS dvc
# MAGIC   , dvc.type as dvc_type
# MAGIC   , dvc.spec as dvc_spec
# MAGIC from aic_data_ods.tlamp.normal_log_webos25
# MAGIC where 1=1
# MAGIC   and date_ym between '2024-01' and '2025-08'
# MAGIC   and context_name="com.webos.app.homeconnect"
# MAGIC   and message_id="NL_HC_DEVICELIST"
# MAGIC
# MAGIC   union all 
# MAGIC
# MAGIC   select 
# MAGIC   substr(X_Device_Platform, 1, 3) as webOS_platform
# MAGIC   , mac_addr
# MAGIC   , explode(from_json(normal_log:dvc_list, 'array<struct<name:string,type:string,spec:string,id:string,label:string>>')) AS dvc
# MAGIC   , dvc.type as dvc_type
# MAGIC   , dvc.spec as dvc_spec
# MAGIC from aic_data_ods.tlamp.normal_log_webos24
# MAGIC where 1=1
# MAGIC   and date_ym between '2024-01' and '2025-08'
# MAGIC   and context_name="com.webos.app.homeconnect"
# MAGIC   and message_id="NL_HC_DEVICELIST"
# MAGIC
# MAGIC   union all 
# MAGIC
# MAGIC   select 
# MAGIC   substr(X_Device_Platform, 1, 3) as webOS_platform
# MAGIC   , mac_addr
# MAGIC   , explode(from_json(normal_log:dvc_list, 'array<struct<name:string,type:string,spec:string,id:string,label:string>>')) AS dvc
# MAGIC   , dvc.type as dvc_type
# MAGIC   , dvc.spec as dvc_spec
# MAGIC from aic_data_ods.tlamp.normal_log_webos23
# MAGIC where 1=1
# MAGIC   and date_ym between '2024-01' and '2025-08'
# MAGIC   and context_name="com.webos.app.homeconnect"
# MAGIC   and message_id="NL_HC_DEVICELIST"  
# MAGIC
# MAGIC   union all 
# MAGIC
# MAGIC   select 
# MAGIC   substr(X_Device_Platform, 1, 3) as webOS_platform
# MAGIC   , mac_addr
# MAGIC   , explode(from_json(normal_log:dvc_list, 'array<struct<name:string,type:string,spec:string,id:string,label:string>>')) AS dvc
# MAGIC   , dvc.type as dvc_type
# MAGIC   , dvc.spec as dvc_spec
# MAGIC from aic_data_ods.tlamp.normal_log_webos22
# MAGIC where 1=1
# MAGIC   and date_ym between '2024-01' and '2025-08'
# MAGIC   and context_name="com.webos.app.homeconnect"
# MAGIC   and message_id="NL_HC_DEVICELIST"  
# MAGIC )
# MAGIC select distinct
# MAGIC   webos_platform, mac_addr, dvc_type, dvc_spec
# MAGIC from base_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from adhoc.heds.1032_cl0