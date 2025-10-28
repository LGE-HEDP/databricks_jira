-- Databricks notebook source
-- MAGIC %md
-- MAGIC [link](http://hlm.lge.com/issue/browse/HEDATAPLFM-896?attachmentSortBy=dateTime&attachmentOrder=asc)
-- MAGIC
-- MAGIC 글로벌 데이터 추출
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 추출 국가 : Global
-- MAGIC
-- MAGIC 플랫폼 :  webOS24
-- MAGIC
-- MAGIC 추출 기간 : 23.01.01 ~ 25.06.30 (webOS 24 전체)
-- MAGIC
-- MAGIC 추출 조건
-- MAGIC 1) context_name :  com.webos.app.homeconnect,   message_id IN :  NL_HC_INPUTLIST    키 값 : dvc_list  (type: audio)   (LG 사운드바 연결 TV만 filtering)
-- MAGIC 2) context_name :  com.palm.app.settings,   message_id IN :  NL_SETTING_CHANGE         키 값 : soundAlive  메뉴 이름 : TV Sound Mode Share   
-- MAGIC
-- MAGIC 목적
-- MAGIC LG 사운드바 연결한 LG TV 사용자 한정, TV에 있는 TV Sound Mode Share 기능을 얼마나 사용하는지 사용률만 계산. (on/off 비율)
-- MAGIC on/off 비율이므로 각 기기별 최종 상태만 확인하는 것이 합리적일 것 같습니다.
-- MAGIC 특성상 mac address 는 필요없습니다.

-- COMMAND ----------

-- DBTITLE 1,struct_type으로 변환
SELECT 
  1,
  dvc_list_struct.type
  dvc_list_struct
FROM (
  SELECT
    from_json(normal_log:dvc_list, 'array<struct<name:string, type:string, spec:string, label:string, id:string>>') as dvc_list_struct
  FROM aic_data_ods.tlamp.normal_log_webos24
  WHERE 
    date_ym = '2025-06'
    AND context_name = 'com.webos.app.homeconnect'
    AND message_id = 'NL_HC_INPUTLIST'
    AND normal_log:dvc_list != '[]'
) t
WHERE array_contains(t.dvc_list_struct.type, 'audio')
LIMIT 500

-- COMMAND ----------

-- DBTITLE 1,데이터 추출_TB0

CREATE TABLE IF NOT EXISTS adhoc.heds.896_cl0 AS
WITH TB0 AS (
  SELECT
    explode(from_json(normal_log:dvc_list, 'array<struct<name:string, type:string, spec:string, label:string, id:string>>')) AS dvc,
    *
  FROM aic_data_ods.tlamp.normal_log_webos24
  WHERE 
    date_ym BETWEEN '2024-03' AND '2025-06'
    AND context_name = 'com.webos.app.homeconnect'
    AND message_id = 'NL_HC_INPUTLIST'
    AND normal_log:dvc_list != '[]'
),
TB1 AS (
  SELECT
    date_ym,
    log_create_time,
    X_Device_Sales_Model AS tv_model,
    DEVICE_NAME AS soc,
    mac_addr,
    dvc.type,
    dvc.label,
    dvc.name,
    ROW_NUMBER() OVER (PARTITION BY mac_addr ORDER BY log_create_time DESC) AS rn
  FROM TB0
  WHERE dvc.type = 'audio'
)
SELECT
  date_ym,
  mac_addr,
  tv_model,
  soc,
  TB1.type as sb_type,
  TB1.label as sb_label,
  TB1.name as sb_name,
  log_create_time as NH_HC_INPUTLIST_last_log_dt
FROM TB1
WHERE rn = 1
;
-- LIMIT 500

-- COMMAND ----------

select count(1)
from adhoc.heds.896_cl0

-- COMMAND ----------

select *
from adhoc.heds.896_cl0

-- COMMAND ----------

-- DBTITLE 1,조회 2
select 
  -- normal_log:soundAlive,
  normal_log, mac_addr, X_User_Number, message_id
  ,*
from aic_data_ods.tlamp.normal_log_webos24
where 1=1
  and date_ym = '2025-06'
  and context_name = 'com.palm.app.settings'
  and message_id in ('NL_SETTING_CHANGE', 'INPUTLIST')
  and normal_log:menu_name ='soundAlive'
limit 500

-- COMMAND ----------



-- COMMAND ----------

-- DBTITLE 1,데이터 추출 TB0
create table if not exists adhoc.heds.896_cl0_2 as
with tb0 as (
  select 
    normal_log:changed_value as soundAlive_status,
    mac_addr,
    log_create_time,
    row_number() over (partition by mac_addr order by log_create_time desc) as rn
  from aic_data_ods.tlamp.normal_log_webos24
  where 1=1
    and date_ym BETWEEN '2024-03' AND '2025-06'
    and context_name = 'com.palm.app.settings'
    and message_id in ('NL_SETTING_CHANGE', 'INPUTLIST')
    and normal_log:menu_name ='soundAlive'
)
select 
  mac_addr, 
  log_create_time as soundAlive_last_log_dt,
  soundAlive_status as soundAlive_last_status
from tb0
where rn = 1

-- COMMAND ----------

select 
  count(1)
from adhoc.heds.896_cl0_2



-- COMMAND ----------

-- DBTITLE 1,데이터결합
select 
  t1.* except (t1.mac_addr), t2.* except (t2.mac_addr)
from adhoc.heds.896_cl0 t1
left join adhoc.heds.896_cl0_2 t2 using (mac_addr)
