--log event start 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_shared_revenue', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.SharedRevenueCompletes;
 

DROP TABLE IF EXISTS SharedRevenueCompletes_temp;

CREATE LOCAL TEMPORARY TABLE SharedRevenueCompletes_temp
(
   nebu_quest_feature_id INT
 , CampaignID INT
 , modified_at TIMESTAMP
 , entity_id INT
 , project_id INT
 , sample_version_id INT
 , yearmo INT
 , sub_panel_id INT
 , project_event_id INT
 , bos_salesorderdetailid VARCHAR(38)
 , country_enum_value_id INT
 , sourcesystemid INT
 , mobiletype VARCHAR(100)
 , platform VARCHAR(100)
 , wh_datemod TIMESTAMP DEFAULT CURRENT_TIMESTAMP
 , CONSTRAINT SharedRevenueCompletes_pk PRIMARY KEY ( nebu_quest_feature_id, entity_id )
)
ON COMMIT PRESERVE ROWS
UNSEGMENTED ALL NODES
;


--insert new rows into temp table
INSERT INTO SharedRevenueCompletes_temp
(
   nebu_quest_feature_id 
 , CampaignID
 , modified_at
 , entity_id
 , project_id
 , yearmo
 , sub_panel_id
 , project_event_id
 , sourcesystemid
)
SELECT 
   f8.feature_id AS nebu_quest_feature_id
 , cmpn.CampaignID
 , f8.modified_at
 , f8.entity_id
 , prj.nebu_project_id AS project_id
 , f8.yearmo
 , f8.sub_panel_id
 , f8.project_event_id
 , CASE 
 	WHEN dl.sub_panel_id IS NOT NULL THEN 15 --new rev share system
 	WHEN cmpn.campaignId IS NOT NULL THEN  6 --old system based on RMT
   ELSE -2 --unknown
   END AS sourcesystemid
FROM (
		SELECT 
			entity_id
			, feature_id
			, modified_at
			, sub_panel_id
			, project_event_id
			, yearmo 
	  	FROM panel.FValues8
	  	WHERE yearmo >= TO_NUMBER(TO_CHAR( ADD_MONTHS( DATE_TRUNC('MONTH', CURRENT_DATE) , -1 ), 'YYYYMM' ) ) --previous month 
	  	AND value = 5 /* completed */
	 ) f8 
INNER JOIN wh.v_nebu_project_attributes prj ON F8.feature_id = prj.nebu_quest_feature_id 
INNER JOIN wh.dim_subpanel_membership spm ON ( f8.entity_id = spm.entity_id AND f8.sub_panel_id = spm.sub_panel_id )
LEFT OUTER JOIN reporting.RMT_Campaign cmpn ON ( spm.campaignid = cmpn.CampaignId AND cmpn.typeid IN ( 5, 11, 12 ) /* Shared Revenue */ )
LEFT OUTER JOIN (SELECT DISTINCT sub_panel_id FROM revshare.Deal) dl ON f8.sub_panel_id = dl.sub_panel_id
WHERE prj.is_revenue = TRUE
AND ( 
	cmpn.CampaignId IS NOT NULL 
	OR dl.sub_panel_id IS NOT NULL 
	)
ORDER BY f8.feature_id, f8.entity_id
;




DROP TABLE IF EXISTS SharedRevenueSession_temp;

CREATE LOCAL TEMPORARY TABLE SharedRevenueSession_temp
ON COMMIT PRESERVE ROWS
AS 
SELECT 
	a.entity_id
	, a.nebu_quest_feature_id
	, UPPER( a.mobiletype ) AS mobiletype
	, CASE 
		WHEN INSTR(LOWER(a.platform),'android') > 0 THEN 'Android' 
		WHEN INSTR(LOWER(a.platform),'win') > 0 THEN 'Windows' 
		WHEN INSTR(LOWER(a.platform),'ipad') > 0 THEN 'iOS' 
		WHEN INSTR(LOWER(a.platform),'iphone') > 0 THEN 'iOS' 
	ELSE UPPER(a.platform) 
	END AS platform
FROM
(
SELECT 
	sr.entity_id
	, sr.nebu_quest_feature_id
	, flt.mobiletype
	, flt.platform
	, ROW_NUMBER() OVER (PARTITION BY si.entity_id, sr.nebu_quest_feature_id ORDER BY p.date_participation_started DESC ) AS rnum_survey
FROM surveyflowcontroller.SessionIdentity si
INNER JOIN surveyflowcontroller.sessionidentityrelevantidcapturedataflat flt ON si.session_identity_id = flt.session_identity_id
INNER JOIN surveyflowcontroller.Participation p ON si.session_identity_id = p.session_identity_id
INNER JOIN SharedRevenueCompletes_temp sr ON ( si.entity_id = sr.entity_id AND sr.nebu_quest_feature_id = p.questionnaire_feature_id )
) a
WHERE a.rnum_survey = 1
;



UPDATE SharedRevenueCompletes_temp 
SET
	mobiletype = src.mobiletype
	, platform = src.platform
FROM SharedRevenueSession_temp src
WHERE SharedRevenueCompletes_temp.entity_id = src.entity_id AND SharedRevenueCompletes_temp.nebu_quest_feature_id = src.nebu_quest_feature_id
;


--update sample_version_id and project_event_id in temp table
UPDATE SharedRevenueCompletes_temp
SET 
	 sample_version_id = pm.sample_version_id
	, project_event_id = pm.project_event_id
FROM (
		SELECT pm1.*
		FROM (SELECT DISTINCT project_id, entity_id FROM SharedRevenueCompletes_temp) AS t1 --Distinct helps query performance
		INNER JOIN panel.Project_members AS pm1
			ON pm1.project_id = t1.project_id AND
			   pm1.entity_id = t1.entity_id
	 ) pm 
WHERE SharedRevenueCompletes_temp.entity_id = pm.entity_id 
AND SharedRevenueCompletes_temp.project_id = pm.project_id
AND SharedRevenueCompletes_temp.project_event_id IS NULL 
;


--update bos_salesorderdetailid in temp table
UPDATE SharedRevenueCompletes_temp
SET bos_salesorderdetailid = lnk.salesorderdetailid
FROM wh.v_salesorderdetail_link lnk
WHERE SharedRevenueCompletes_temp.project_event_id = lnk.project_event_id
AND SharedRevenueCompletes_temp.bos_salesorderdetailid IS NULL
;

--update country_enum_value_id in temp table
DROP TABLE IF EXISTS SharedRevenueCountry_temp;

CREATE LOCAL TEMPORARY TABLE SharedRevenueCountry_temp
ON COMMIT PRESERVE ROWS 
AS
SELECT 
	f8.entity_id
	, f8.nebu_quest_feature_id
	, ctry.country_enum_value_id
 FROM SharedRevenueCompletes_temp f8
 INNER JOIN bi.dim_sub_panel sp ON f8.sub_panel_id = sp.sub_panel_id
 INNER JOIN bi.dim_respondent r ON f8.entity_id = r.entity_id
 INNER JOIN bi.fact_combined_membership fcm 
		ON 
			( 
				r.dim_respondent_key = fcm.dim_respondent_key AND 
				sp.dim_sub_panel_key = fcm.dim_sub_panel_key AND 
				f8.modified_at BETWEEN fcm.start_date AND COALESCE(fcm.end_date, CURRENT_TIMESTAMP)
			)
INNER JOIN bi.dim_country ctry ON fcm.dim_country_key = ctry.dim_country_key
;


UPDATE SharedRevenueCompletes_temp
SET country_enum_value_id = src.country_enum_value_id
FROM SharedRevenueCountry_temp src
WHERE SharedRevenueCompletes_temp.nebu_quest_feature_id = src.nebu_quest_feature_id
AND SharedRevenueCompletes_temp.entity_id =  src.entity_id
;


--merge rows from temp table into target
MERGE INTO wh.SharedRevenueCompletes tgt
USING SharedRevenueCompletes_temp src
 ON (
 			tgt.nebu_quest_feature_id = src.nebu_quest_feature_id AND
 			tgt. entity_id = src.entity_id
 		)
WHEN MATCHED THEN UPDATE
	SET 
			 CampaignID = src.CampaignID 
			 , modified_at = src.modified_at 
			 , project_id = src.project_id 
			 , sample_version_id = src.sample_version_id 
			 , yearmo = src.yearmo 
			 , sub_panel_id = src.sub_panel_id 
			 , project_event_id = src.project_event_id 
			 , bos_salesorderdetailid = src.bos_salesorderdetailid 
			 , country_enum_value_id = src.country_enum_value_id
			 , sourcesystemid = src.sourcesystemid
			 , mobiletype = src.mobiletype
			 , platform = src.platform
			 , wh_datemod = SYSDATE 
WHEN NOT MATCHED THEN 
INSERT 
	( 
	 nebu_quest_feature_id 
	 , CampaignID 
	 , modified_at 
	 , entity_id 
	 , project_id 
	 , sample_version_id 
	 , yearmo 
	 , sub_panel_id 
	 , project_event_id 
	 , bos_salesorderdetailid 
	 , country_enum_value_id
	 , mobiletype
	 , platform
	 , sourcesystemid
) 
VALUES
( 
   src.nebu_quest_feature_id 
 , src.CampaignID 
 , src.modified_at 
 , src.entity_id 
 , src.project_id 
 , src.sample_version_id 
 , src.yearmo 
 , src.sub_panel_id 
 , src.project_event_id 
 , src.bos_salesorderdetailid
 , src.country_enum_value_id 
 , src.mobiletype
 , src.platform
 , src.sourcesystemid
 ) 
;


---------------------------------------------
--FILL IN salesorderdetailid
--FOR any records where it is missing
---------------------------------------------
UPDATE wh.SharedRevenueCompletes 
SET 
	bos_salesorderdetailid = src.salesorderdetailid
	, wh_datemod = SYSDATE
FROM 
	( 
	SELECT project_event_id, salesorderdetailid
	FROM wh.v_salesorderdetail_link 
	WHERE project_event_id IS NOT NULL 
	AND salesorderdetailid IS NOT NULL --added 2014-10-16
	) src
WHERE wh.SharedRevenueCompletes.project_event_id = src.project_event_id
AND wh.SharedRevenueCompletes.bos_salesorderdetailid IS NULL
AND wh.SharedRevenueCompletes.project_event_id IS NOT NULL
;


---------------------------------------------
--FILL IN country
--FOR any records where it is missing
---------------------------------------------
DROP TABLE IF EXISTS SharedRevenueCountry_temp;

CREATE LOCAL TEMPORARY TABLE SharedRevenueCountry_temp
ON COMMIT PRESERVE ROWS 
AS
SELECT 
	shrc.entity_id
	, shrc.nebu_quest_feature_id
	, ctry.country_enum_value_id
FROM wh.SharedRevenueCompletes shrc
INNER JOIN bi.dim_respondent r ON shrc.entity_id = r.entity_id
INNER JOIN bi.dim_sub_panel sp ON shrc.sub_panel_id = sp.sub_panel_id
INNER JOIN bi.fact_combined_membership fcm 
		ON 
			( 
				r.dim_respondent_key = fcm.dim_respondent_key AND 
				sp.dim_sub_panel_key = fcm.dim_sub_panel_key AND 
				shrc.modified_at BETWEEN fcm.start_date AND COALESCE(fcm.end_date, CURRENT_TIMESTAMP)
			)
INNER JOIN bi.dim_country ctry ON fcm.dim_country_key = ctry.dim_country_key
WHERE 
	( shrc.country_enum_value_id IS NULL OR shrc.country_enum_value_id = -1 )
AND ctry.country_enum_value_id <> -1
;


UPDATE  wh.SharedRevenueCompletes
SET 
	country_enum_value_id = src.country_enum_value_id
	, wh_datemod = SYSDATE
FROM SharedRevenueCountry_temp src
WHERE wh.SharedRevenueCompletes.nebu_quest_feature_id = src.nebu_quest_feature_id
AND wh.SharedRevenueCompletes.entity_id = src.entity_id
;



--log event end 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_shared_revenue', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.SharedRevenueCompletes;