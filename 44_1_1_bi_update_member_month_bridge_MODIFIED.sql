--log event start
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_member_month_bridge', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM bi.member_month_bridge;


DROP TABLE IF EXISTS month_list;

CREATE LOCAL TEMPORARY TABLE month_list
ON COMMIT PRESERVE ROWS 
AS
SELECT 
	dim_month_key
	, month_start_date
	, yearmo 
FROM bi.dim_month
WHERE month_start_date 
	BETWEEN 
		( 
			SELECT MAX( m.month_start_date ) 
			FROM bi.member_month_bridge brdg 
			INNER JOIN bi.dim_month m ON brdg.dim_month_key = m.dim_month_key
		)
		AND
		CURRENT_DATE
ORDER BY dim_month_key
; 

TRUNCATE TABLE bi_stage.member_month_bridge;

  
--active records with end date
INSERT INTO bi_stage.member_month_bridge
	(
	  fact_respondent_membership_key
	, fact_sub_panel_membership_key
	, dim_respondent_key
	, dim_sub_panel_key
	, dim_membership_status_change_type_key
	, dim_country_key
	, current_combined_dim_membership_status_key  
	, dim_month_key
	, first_month_flag 
	, month_start_flag 
	, month_end_flag
	, is_active
	, panel_size_delta
	)
SELECT 
	  fcm.fact_respondent_membership_key
	, fcm.fact_sub_panel_membership_key
	, fcm.dim_respondent_key
	, fcm.dim_sub_panel_key
	, fcm.dim_membership_status_change_type_key
	, fcm.dim_country_key
	, fcm.current_combined_dim_membership_status_key
	, m.dim_month_key
	, CASE WHEN DATE_TRUNC('month', fcm.start_date ) = m.month_start_date THEN TRUE ELSE FALSE END AS first_month_flag
	, CASE WHEN fcm.start_date <= m.month_start_date THEN TRUE ELSE FALSE END AS month_start_flag
	, CASE WHEN LEAST( m.month_end_date, CURRENT_TIMESTAMP ) BETWEEN fcm.start_date AND fcm.end_date THEN TRUE ELSE FALSE END AS month_end_flag
	, ms.is_active
	, fcm.panel_size_delta
FROM bi.fact_combined_membership fcm
INNER JOIN bi.dim_month m ON ( fcm.start_date, fcm.end_date ) OVERLAPS ( m.month_start_date, m.month_end_date )
INNER JOIN month_list ml on m.dim_month_key = ml.dim_month_key
INNER JOIN bi.dim_membership_status ms ON fcm.current_combined_dim_membership_status_key = ms.dim_membership_status_key
WHERE ms.is_active = TRUE 
AND fcm.end_date IS NOT NULL 
;


--active records without end_date
--create a row for each month in which the respondent is active
INSERT INTO bi_stage.member_month_bridge
	(
	  fact_respondent_membership_key
	, fact_sub_panel_membership_key
	, dim_respondent_key
	, dim_sub_panel_key
	, dim_membership_status_change_type_key
	, dim_country_key
	, current_combined_dim_membership_status_key  
	, dim_month_key
	, first_month_flag 
	, month_start_flag 
	, month_end_flag
	, is_active
	, panel_size_delta
	)
SELECT 
	  fcm.fact_respondent_membership_key
	, fcm.fact_sub_panel_membership_key
	, fcm.dim_respondent_key
	, fcm.dim_sub_panel_key
	, fcm.dim_membership_status_change_type_key
	, fcm.dim_country_key
    , fcm.current_combined_dim_membership_status_key
	, m.dim_month_key
	, CASE WHEN DATE_TRUNC('month', fcm.start_date ) = m.month_start_date THEN TRUE ELSE FALSE END AS first_month_flag
	, CASE WHEN fcm.start_date <= m.month_start_date THEN TRUE ELSE FALSE END AS month_start_flag
	, CASE WHEN LEAST( m.month_end_date, CURRENT_TIMESTAMP ) BETWEEN fcm.start_date AND CURRENT_TIMESTAMP THEN TRUE ELSE FALSE END AS month_end_flag
	, ms.is_active
	, fcm.panel_size_delta
FROM bi.fact_combined_membership fcm
INNER JOIN bi.dim_month m ON ( fcm.start_date, CURRENT_TIMESTAMP ) OVERLAPS ( m.month_start_date, m.month_end_date )
INNER JOIN month_list ml on m.dim_month_key = ml.dim_month_key
INNER JOIN bi.dim_membership_status ms ON fcm.current_combined_dim_membership_status_key = ms.dim_membership_status_key
WHERE fcm.end_date IS  NULL 
AND ms.is_active = TRUE 
;




--inactive records, with or without end_date
--create a single row for the month in which the event happened
INSERT INTO bi_stage.member_month_bridge
	(
	  fact_respondent_membership_key
	, fact_sub_panel_membership_key
	, dim_respondent_key
	, dim_sub_panel_key
	, dim_membership_status_change_type_key
	, dim_country_key
	, current_combined_dim_membership_status_key  
	, dim_month_key
	, first_month_flag 
	, month_start_flag 
	, month_end_flag
	, is_active
	, panel_size_delta
	)
SELECT 
	  fcm.fact_respondent_membership_key
	, fcm.fact_sub_panel_membership_key
	, fcm.dim_respondent_key
	, fcm.dim_sub_panel_key
	, fcm.dim_membership_status_change_type_key
	, fcm.dim_country_key
	, fcm.current_combined_dim_membership_status_key
	, m.dim_month_key
	, TRUE AS first_month_flag
	, CASE WHEN fcm.start_date = m.month_start_date THEN TRUE ELSE FALSE END AS month_start_flag
	, TRUE AS month_end_flag
	, ms.is_active
	, fcm.panel_size_delta
FROM bi.fact_combined_membership fcm
INNER JOIN bi.dim_month m ON fcm.start_date BETWEEN m.month_start_date AND m.month_end_date
INNER JOIN month_list ml on m.dim_month_key = ml.dim_month_key
INNER JOIN bi.dim_membership_status ms ON fcm.current_combined_dim_membership_status_key = ms.dim_membership_status_key
WHERE  ms.is_active = FALSE 
;


DELETE FROM bi.member_month_bridge 
WHERE dim_month_key IN 
	( 
		SELECT DISTINCT dim_month_key 
		FROM bi_stage.member_month_bridge 
	)
;

INSERT /*+direct*/ INTO bi.member_month_bridge
	(
	  fact_respondent_membership_key
	, fact_sub_panel_membership_key
	, dim_respondent_key
	, dim_sub_panel_key
	, dim_membership_status_change_type_key
	, dim_country_key
    , current_combined_dim_membership_status_key  
	, dim_month_key
	, first_month_flag 
	, month_start_flag 
	, month_end_flag
	, is_active
	, panel_size_delta
	)
SELECT 
      fact_respondent_membership_key
	, fact_sub_panel_membership_key
	, dim_respondent_key
	, dim_sub_panel_key
	, dim_membership_status_change_type_key
	, dim_country_key
	, current_combined_dim_membership_status_key
	, dim_month_key
	, first_month_flag 
	, month_start_flag 
	, month_end_flag
	, is_active
	, panel_size_delta
FROM bi_stage.member_month_bridge
;


--log event end

INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_member_month_bridge', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM bi.member_month_bridge;