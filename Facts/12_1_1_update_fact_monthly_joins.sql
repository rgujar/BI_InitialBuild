--update monthly joins fact table

--log event start
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_fact_monthly_joins', 'start', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_fact_monthly_joins', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.fact_monthly_joins;

TRUNCATE TABLE wh.fact_monthly_joins;

INSERT INTO wh.fact_monthly_joins
  (
  campaignid,
  sub_panel_id,
  country_enum_value_id,
  join_month,
  joins     
 )
SELECT
  spm.campaignid,
  spm.sub_panel_id, 
  r.country_enum_value_id, 
  DATE_TRUNC( 'MONTH', spm.date_membership_started ) AS join_month,
  COUNT(*) AS joins
FROM wh.dim_subpanel_membership spm
INNER JOIN wh.dim_respondent r ON spm.entity_id = r.entity_id
WHERE r.country_enum_value_id IS NOT NULL
AND spm.sub_panel_id IS NOT NULL
AND spm.campaignid IS NOT NULL
AND spm.date_membership_started IS NOT NULL
GROUP BY 
 spm.campaignid,
 spm.sub_panel_id, 
 r.country_enum_value_id,
 DATE_TRUNC( 'MONTH', spm.date_membership_started )
;

--log event end
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_fact_monthly_joins', 'end', SYSDATE );       
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_fact_monthly_joins', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.fact_monthly_joins;