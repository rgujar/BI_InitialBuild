
--log event start
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES ( 'fact_recruitment_cohort_activity', 'start', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'fact_recruitment_cohort_activity', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.fact_recruitment_cohort_activity; 

              
DROP TABLE IF EXISTS fact_recruitment_cohort_activity_temp;

CREATE LOCAL TEMPORARY TABLE fact_recruitment_cohort_activity_temp
(
  dim_respondent_source_id INT NOT NULL,
  dim_country_id           INT NOT NULL,
  sub_panel_id             INT NOT NULL,
  join_month_id            INT NOT NULL,
  activity_month_id        INT NOT NULL,
  nPanelists_Responded     INT,
  nPanelists_Completed     INT,
  nInvited_Direct          INT,
  nInvited_DirectReminder  INT,
  nInvited_General         INT,
  nInvited_Preferred       INT,
  nStarted                 INT,
  nBounced                 INT,
  nRefused                 INT,
  nCompleted               INT,
  nPartial_Complete        INT,
  nQuota_Full              INT,
  nScreenout               INT,
  wh_datemod               TIMESTAMP DEFAULT SYSDATE,
  CONSTRAINT fact_recruitment_cohort_activity_pk PRIMARY KEY ( dim_respondent_source_id, dim_country_id, sub_panel_id, join_month_id, activity_month_id )
)
ON COMMIT PRESERVE ROWS
UNSEGMENTED ALL NODES
;


DROP TABLE IF EXISTS responses_temp;

CREATE LOCAL TEMPORARY TABLE responses_temp
ON COMMIT PRESERVE ROWS
AS
SELECT
  TO_NUMBER(TO_CHAR( spm.date_membership_started, 'YYYYMM' ) ) AS join_yearmo
  , spm.dim_respondent_source_id
  , f8.yearmo AS activity_yearmo
  , ctry.dim_country_id
  , spm.sub_panel_id
  , COUNT( DISTINCT CASE WHEN f8.value > 1 THEN f8.entity_id ELSE NULL END ) AS nPanelists_Responded
  , COUNT( DISTINCT CASE WHEN f8.value = 5 THEN f8.entity_id ELSE NULL END ) AS nPanelists_Completed
  , SUM( CASE WHEN f8.value = 2 THEN 1 ELSE 0 END ) AS nStarted
  , SUM( CASE WHEN f8.value = 3 THEN 1 ELSE 0 END ) AS nBounced
  , SUM( CASE WHEN f8.value = 4 THEN 1 ELSE 0 END ) AS nRefused
  , SUM( CASE WHEN f8.value = 5 THEN 1 ELSE 0 END ) AS nCompleted
  , SUM( CASE WHEN f8.value = 6 THEN 1 ELSE 0 END ) AS nPartial_Complete
  , SUM( CASE WHEN f8.value = 7 THEN 1 ELSE 0 END ) AS nQuota_Full
  , SUM( CASE WHEN f8.value = 8 THEN 1 ELSE 0 END ) AS nScreenout
FROM (
		SELECT *
		FROM wh.dim_subpanel_membership
		WHERE date_membership_started IS NOT NULL AND   
		 	  dim_respondent_source_id  IS NOT NULL
	 ) spm
INNER JOIN (
				SELECT * 
				FROM panel.fvalues1 
				WHERE feature_id = 17 --Country
		   ) r ON spm.entity_id = r.entity_id
INNER JOIN wh.dim_country ctry ON r.value = ctry.country_enum_value_id
INNER JOIN (
				SELECT *
				FROM panel.fvalues8 
				WHERE yearmo BETWEEN TO_NUMBER(TO_CHAR(ADD_MONTHS( DATE_TRUNC('MONTH', SYSDATE), -1),'YYYYMM')) AND TO_NUMBER(TO_CHAR( SYSDATE,'YYYYMM'))
		   ) f8 ON spm.entity_id = f8.entity_id AND spm.sub_panel_id = f8.sub_panel_id
INNER JOIN wh.v_project prj ON f8.feature_id = prj.nebu_quest_feature_id
WHERE prj.is_Revenue = TRUE
GROUP BY
   TO_NUMBER(TO_CHAR( spm.date_membership_started, 'YYYYMM' ) )
 , spm.dim_respondent_source_id
 , f8.yearmo
 , ctry.dim_country_id
 , spm.sub_panel_id
ORDER BY
   TO_NUMBER(TO_CHAR( spm.date_membership_started, 'YYYYMM' ) )
 , spm.dim_respondent_source_id
 , f8.yearmo
 , ctry.dim_country_id
 , spm.sub_panel_id 
 ;

DROP TABLE IF EXISTS invites_temp;

CREATE LOCAL TEMPORARY TABLE invites_temp
ON COMMIT PRESERVE ROWS
AS
SELECT
    TO_NUMBER(TO_CHAR( spm.date_membership_started, 'YYYYMM' ) ) AS join_yearmo
  , spm.dim_respondent_source_id
  , mjm.yearmo AS activity_yearmo
  , ctry.dim_country_id
  , spm.sub_panel_id
  , SUM( CASE WHEN mj.mail_functionality_id = 1 AND prj.is_Revenue = TRUE THEN 1 ELSE 0 END ) AS nInvited_Direct
  , SUM( CASE WHEN mj.mail_functionality_id = 2 AND prj.is_Revenue = TRUE THEN 1 ELSE 0 END ) AS nInvited_DirectReminder
  , SUM( CASE WHEN mj.mail_functionality_id = 10 THEN 1 ELSE 0 END ) AS nInvited_General
  , SUM( CASE WHEN mj.mail_functionality_id = 11 AND prj.is_Revenue = TRUE THEN 1 ELSE 0 END ) AS nInvited_Preferred
FROM (
		SELECT *
		FROM wh.dim_subpanel_membership
		WHERE date_membership_started IS NOT NULL AND
			  dim_respondent_source_id IS NOT NULL
	 ) spm
INNER JOIN (
				SELECT * 
				FROM panel.fvalues1 
				WHERE feature_id = 17
		   ) r ON spm.entity_id = r.entity_id
INNER JOIN wh.dim_country ctry ON r.value = ctry.country_enum_value_id		   
INNER JOIN (
			   SELECT *
			   FROM panel.Mail_job_members 
			   WHERE is_mail_sent = 1 AND --mail sent successfully
					 yearmo BETWEEN TO_NUMBER(TO_CHAR(ADD_MONTHS( DATE_TRUNC('MONTH', SYSDATE), -1),'YYYYMM')) AND TO_NUMBER(TO_CHAR( SYSDATE,'YYYYMM'))
		   ) mjm ON spm.entity_id = mjm.entity_id
INNER JOIN panel.mail_jobs mj ON mjm.mail_job_id = mj.mail_job_id
INNER JOIN wh.v_project prj ON mj.project_id = prj.nebu_project_id
WHERE prj.is_Revenue = TRUE
GROUP BY
    TO_NUMBER(TO_CHAR( spm.date_membership_started, 'YYYYMM' ) )
  , spm.dim_respondent_source_id
  , mjm.yearmo
  , ctry.dim_country_id
  , spm.sub_panel_id
ORDER BY
    TO_NUMBER(TO_CHAR( spm.date_membership_started, 'YYYYMM' ) )
  , spm.dim_respondent_source_id
  , mjm.yearmo
  , ctry.dim_country_id
  , spm.sub_panel_id    
;


--insert data for previous month and current month into temp table
INSERT INTO fact_recruitment_cohort_activity_temp
(
  join_month_id,
  dim_respondent_source_id,
  activity_month_id,
  dim_country_id,
  sub_panel_id,
  nPanelists_Responded,
  nPanelists_Completed,
  nInvited_Direct,
  nInvited_DirectReminder,
  nInvited_General,
  nInvited_Preferred,
  nStarted,
  nBounced,
  nRefused,
  nCompleted,
  nPartial_Complete,
  nQuota_Full,
  nScreenout,
  wh_datemod
 )
SELECT 
   jm.month_id AS join_month_id
 , x.dim_respondent_source_id
 , am.month_id AS activity_month_id
 , x.dim_country_id
 , x.sub_panel_id           
 , x.nPanelists_Responded     
 , x.nPanelists_Completed     
 , x.nInvited_Direct          
 , x.nInvited_DirectReminder  
 , x.nInvited_General         
 , x.nInvited_Preferred       
 , x.nStarted                 
 , x.nBounced                 
 , x.nRefused                 
 , x.nCompleted               
 , x.nPartial_Complete        
 , x.nQuota_Full              
 , x.nScreenout             
 , SYSDATE AS wh_datemod
FROM
(
SELECT
 COALESCE( rsp.join_yearmo, inv.join_yearmo ) AS join_yearmo,
 COALESCE( rsp.dim_respondent_source_id, inv.dim_respondent_source_id ) AS dim_respondent_source_id,
 COALESCE( rsp.activity_yearmo, inv.activity_yearmo ) AS activity_yearmo,
 COALESCE( rsp.dim_country_id, inv.dim_country_id ) AS dim_country_id,
 COALESCE( rsp.sub_panel_id, inv.sub_panel_id ) AS sub_panel_id, 
 COALESCE( rsp.nPanelists_Responded     , 0 ) AS nPanelists_Responded    ,
 COALESCE( rsp.nPanelists_Completed     , 0 ) AS nPanelists_Completed    ,
 COALESCE( inv.nInvited_Direct          , 0 ) AS nInvited_Direct         ,
 COALESCE( inv.nInvited_DirectReminder  , 0 ) AS nInvited_DirectReminder ,
 COALESCE( inv.nInvited_General         , 0 ) AS nInvited_General        ,
 COALESCE( inv.nInvited_Preferred       , 0 ) AS nInvited_Preferred      ,
 COALESCE( rsp.nStarted                 , 0 ) AS nStarted                ,
 COALESCE( rsp.nBounced                 , 0 ) AS nBounced                ,
 COALESCE( rsp.nRefused                 , 0 ) AS nRefused                ,
 COALESCE( rsp.nCompleted               , 0 ) AS nCompleted              ,
 COALESCE( rsp.nPartial_Complete        , 0 ) AS nPartial_Complete       ,
 COALESCE( rsp.nQuota_Full              , 0 ) AS nQuota_Full             ,
 COALESCE( rsp.nScreenout               , 0 ) AS nScreenout 
FROM responses_temp rsp
FULL OUTER JOIN invites_temp inv
ON (
   rsp.join_yearmo = inv.join_yearmo AND
   rsp.dim_respondent_source_id = inv.dim_respondent_source_id AND
   rsp.activity_yearmo = inv.activity_yearmo AND
   rsp.dim_country_id = inv.dim_country_id AND
   rsp.sub_panel_id = inv.sub_panel_id
  )
) x
INNER JOIN wh.dim_month am ON x.activity_yearmo = am.yearmo
INNER JOIN wh.dim_month jm ON x.join_yearmo = jm.yearmo
;



--insert historical data into temp table
INSERT INTO fact_recruitment_cohort_activity_temp
(
  join_month_id,
  dim_respondent_source_id,
  activity_month_id,
  dim_country_id,
  sub_panel_id,
  nPanelists_Responded,
  nPanelists_Completed,
  nInvited_Direct,
  nInvited_DirectReminder,
  nInvited_General,
  nInvited_Preferred,
  nStarted,
  nBounced,
  nRefused,
  nCompleted,
  nPartial_Complete,
  nQuota_Full,
  nScreenout,
  wh_datemod
 )
SELECT
  a.join_month_id,
  a.dim_respondent_source_id,
  a.activity_month_id,
  a.dim_country_id,
  a.sub_panel_id,
  a.nPanelists_Responded,
  a.nPanelists_Completed,
  a.nInvited_Direct,
  a.nInvited_DirectReminder,
  a.nInvited_General,
  a.nInvited_Preferred,
  a.nStarted,
  a.nBounced,
  a.nRefused,
  a.nCompleted,
  a.nPartial_Complete,
  a.nQuota_Full,
  a.nScreenout,
  a.wh_datemod
FROM wh.fact_recruitment_cohort_activity a
WHERE activity_month_id NOT IN  ( SELECT DISTINCT activity_month_id FROM fact_recruitment_cohort_activity_temp ) 
;



--truncate the target table
TRUNCATE TABLE  wh.fact_recruitment_cohort_activity;

--insert data from temp table into target
INSERT INTO  wh.fact_recruitment_cohort_activity
(
  join_month_id,
  dim_respondent_source_id,
  activity_month_id,
  dim_country_id,
  sub_panel_id,
  nPanelists_Responded,
  nPanelists_Completed,
  nInvited_Direct,
  nInvited_DirectReminder,
  nInvited_General,
  nInvited_Preferred,
  nStarted,
  nBounced,
  nRefused,
  nCompleted,
  nPartial_Complete,
  nQuota_Full,
  nScreenout,
  wh_datemod
 )
SELECT
	join_month_id,
  dim_respondent_source_id,
  activity_month_id,
  dim_country_id,
  sub_panel_id,
  nPanelists_Responded,
  nPanelists_Completed,
  nInvited_Direct,
  nInvited_DirectReminder,
  nInvited_General,
  nInvited_Preferred,
  nStarted,
  nBounced,
  nRefused,
  nCompleted,
  nPartial_Complete,
  nQuota_Full,
  nScreenout,
  wh_datemod
FROM fact_recruitment_cohort_activity_temp
;



--log event end
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES ( 'fact_recruitment_cohort_activity', 'end', SYSDATE );   
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'fact_recruitment_cohort_activity', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.fact_recruitment_cohort_activity;