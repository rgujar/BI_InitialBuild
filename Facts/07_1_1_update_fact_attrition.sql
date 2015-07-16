--log event start
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'fact_attrition', 'start', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'fact_attrition', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.fact_attrition;

CREATE LOCAL TEMPORARY TABLE fact_attrition_temp
(
campaignid          NUMERIC(38,0),
sub_panel_id        NUMERIC(38,0),
country_enum_value_id NUMERIC(18,0),
join_month_id       INT,
join_month          TIMESTAMP,
end_month_id        INT,
end_month           TIMESTAMP,
membershipstatusid  NUMERIC(38,0),
nbr_membership_status INT,
wh_datemod          TIMESTAMP DEFAULT SYSDATE 
)
ON COMMIT PRESERVE ROWS
UNSEGMENTED ALL NODES
;

TRUNCATE TABLE fact_attrition_temp;


INSERT INTO fact_attrition_temp
(
 campaignid,
 sub_panel_id,
 country_enum_value_id,
 join_month_id,
 join_month,
 end_month_id,
 end_month,
 membershipstatusid,
 nbr_membership_status
)
SELECT 
 campaignid,
 sub_panel_id,
 country_enum_value_id,
 join_month_id,
 join_month,
 end_month_id,
 end_month,
 membershipstatusid,
 nbr_membership_status
FROM wh.v_attrition;




TRUNCATE TABLE wh.fact_attrition;


INSERT INTO wh.fact_attrition
(
campaignid,
 sub_panel_id,
 country_enum_value_id,
 join_month_id,
 join_month,
 end_month_id,
 end_month,
 membershipstatusid,
 nbr_membership_status
)
SELECT
 campaignid,
 sub_panel_id,
 country_enum_value_id,
 join_month_id,
 join_month,
 end_month_id,
 end_month,
 membershipstatusid,
 nbr_membership_status
FROM fact_attrition_temp;



--log event end
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'fact_attrition', 'end', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'fact_attrition', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.fact_attrition;