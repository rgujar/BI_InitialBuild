--Update subpanel membership dimension ( wh.dim_subpanel_membership )

--log process start
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_dim_subpanel_membership', 'start', SYSDATE );
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_dim_subpanel_membership', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.dim_subpanel_membership;
--subpanel boolean: Create rows
DROP TABLE IF EXISTS new_members_temp;

CREATE LOCAL TEMPORARY TABLE new_members_temp
 ON COMMIT PRESERVE ROWS
 AS
 SELECT
   src.entity_id,
   spf.sub_panel_id,
   src.value AS is_active
  FROM panel.fvalues1 AS src
  INNER JOIN panel.Sub_panel_features spf ON ( spf.feature_id = src.feature_id AND spf.feature_order = 1 )
  WHERE src.wh_datemod > CURRENT_DATE - INTERVAL '10 DAY'
  ORDER BY
  	  src.entity_id
  	, spf.sub_panel_id
  ;

 --delete rows that exist in target table
 DELETE FROM new_members_temp
  WHERE (entity_id, sub_panel_id ) IN
     (SELECT entity_id, sub_panel_id  FROM wh.dim_subpanel_membership)
  ;



/*
--commented out to try the insert

 MERGE INTO wh.dim_subpanel_membership tgt
 USING new_members_temp src
  ON ( src.entity_id = tgt.entity_id AND src.sub_panel_id = tgt.sub_panel_id )
  WHEN NOT MATCHED THEN INSERT
  		(
  		  entity_id
  		 , sub_panel_id
  		 , is_active
  		 )
  VALUES
  	  (
  		   src.entity_id
  		 , src.sub_panel_id
  		 , src.is_active
  		 )
;
*/


INSERT INTO  wh.dim_subpanel_membership
 (
	  entity_id
	 , sub_panel_id
	 , is_active
	 )
SELECT
    entity_id
	 , sub_panel_id
	 , is_active
FROM new_members_temp
WHERE ( entity_id, sub_panel_id )
NOT IN
	(SELECT entity_id, sub_panel_id
	 FROM wh.dim_subpanel_membership )
;



--membership status
UPDATE wh.dim_subpanel_membership
 SET
     membershipstatusid = src.membershipstatusid,
     date_membership_status_mod = src.date_membership_status_mod,
     wh_datemod = SYSDATE
 FROM
 (
 SELECT
  fv.entity_id,
  spf.sub_panel_id,
  fv.value AS membershipstatusid,
  fv.modified_at AS date_membership_status_mod
 FROM panel.fvalues1 fv
 INNER JOIN panel.Sub_panel_features spf ON spf.feature_id = fv.feature_id
 INNER JOIN wh.subpanel_feature_target ft ON ( spf.feature_order = ft.feature_order AND ft.feature_order = 106 )
 WHERE fv.wh_datemod >= CURRENT_DATE - INTERVAL '10 DAY'
 ) src
 WHERE wh.dim_subpanel_membership.entity_id = src.entity_id
 AND wh.dim_subpanel_membership.sub_panel_id = src.sub_panel_id
 AND
 (
 wh.dim_subpanel_membership.membershipstatusid IS NULL
 OR
 wh.dim_subpanel_membership.membershipstatusid <> src.membershipstatusid
 )
;


--membership start date
UPDATE wh.dim_subpanel_membership
SET
     date_membership_started = src.value
   , wh_datemod = SYSDATE
FROM (
      SELECT fv.entity_id, spf.sub_panel_id, fv.value
       FROM panel.fvalues3 fv
       INNER JOIN panel.Sub_panel_features spf ON spf.feature_id = fv.feature_id
       INNER JOIN wh.subpanel_feature_target ft ON ( spf.feature_order = ft.feature_order AND ft.feature_order = 3 )
       WHERE fv.wh_datemod >= CURRENT_DATE - INTERVAL '10 DAY'
       ) src
WHERE wh.dim_subpanel_membership.entity_id = src.entity_id
AND wh.dim_subpanel_membership.sub_panel_id = src.sub_panel_id
AND
 (
  wh.dim_subpanel_membership.date_membership_started IS NULL
  OR
  wh.dim_subpanel_membership.date_membership_started <> src.value
 )
;



--SELECT 'membership start date' as step_name, COUNT(*) AS number_of_constraint_violations FROM ( SELECT analyze_constraints('wh.dim_subpanel_membership') ) x;

--membership end date
UPDATE wh.dim_subpanel_membership
SET
    date_membership_ended = src.value
   , wh_datemod = SYSDATE
FROM (
      SELECT fv.entity_id, spf.sub_panel_id, fv.value
       FROM panel.fvalues3 fv
       INNER JOIN panel.Sub_panel_features spf ON spf.feature_id = fv.feature_id
       INNER JOIN wh.subpanel_feature_target ft ON ( spf.feature_order = ft.feature_order AND ft.feature_order = 4 )
       WHERE fv.wh_datemod >= CURRENT_DATE - INTERVAL '10 DAY'
       ) src
WHERE wh.dim_subpanel_membership.entity_id = src.entity_id
AND wh.dim_subpanel_membership.sub_panel_id = src.sub_panel_id
AND
 (
  wh.dim_subpanel_membership.date_membership_ended IS NULL
  OR
  wh.dim_subpanel_membership.date_membership_ended <> src.value
 )
;



--campaignid
UPDATE wh.dim_subpanel_membership
SET
     campaignid = src.value
   , wh_datemod = SYSDATE
FROM (
      SELECT fv.entity_id, spf.sub_panel_id, fv.value
       FROM panel.fvalues1 fv
       INNER JOIN panel.Sub_panel_features spf ON spf.feature_id = fv.feature_id
       INNER JOIN wh.subpanel_feature_target ft ON ( spf.feature_order = ft.feature_order AND ft.feature_order = 101 )
       WHERE fv.wh_datemod >= CURRENT_DATE - INTERVAL '10 DAY'
       ) src
WHERE wh.dim_subpanel_membership.entity_id = src.entity_id
AND wh.dim_subpanel_membership.sub_panel_id = src.sub_panel_id
AND  wh.dim_subpanel_membership.campaignid IS NULL
;




--external system memberid
UPDATE wh.dim_subpanel_membership
SET
		  external_system_member_id = src.value
		, wh_datemod = SYSDATE
FROM (
      SELECT fv.entity_id, spf.sub_panel_id, fv.value
       FROM panel.fvalues4 fv
       INNER JOIN panel.Sub_panel_features spf ON spf.feature_id = fv.feature_id
       INNER JOIN wh.subpanel_feature_target ft ON ( spf.feature_order = ft.feature_order AND ft.feature_order = 103 )
       WHERE fv.wh_datemod >= CURRENT_DATE - INTERVAL '10 DAY'
       ) src
WHERE wh.dim_subpanel_membership.entity_id = src.entity_id
AND wh.dim_subpanel_membership.sub_panel_id = src.sub_panel_id
AND wh.dim_subpanel_membership.external_system_member_id IS NULL
;



--Respondent Source
UPDATE wh.dim_subpanel_membership
SET
		  respondent_source = src.value
		, wh_datemod = SYSDATE
FROM (
      SELECT fv.entity_id, spf.sub_panel_id, fv.value
       FROM panel.fvalues4 fv
       INNER JOIN panel.Sub_panel_features spf ON spf.feature_id = fv.feature_id
       INNER JOIN wh.subpanel_feature_target ft ON ( spf.feature_order = ft.feature_order AND ft.feature_order = 2 )
       WHERE fv.wh_datemod >= CURRENT_DATE - INTERVAL '10 DAY'
       ) src
WHERE wh.dim_subpanel_membership.entity_id = src.entity_id
AND  wh.dim_subpanel_membership.sub_panel_id = src.sub_panel_id
AND  wh.dim_subpanel_membership.respondent_source IS NULL
;




--privacy_terms_consent_date
--added 2012-04-17
UPDATE wh.dim_subpanel_membership
SET
		  privacy_terms_consent_date = src.value
		, wh_datemod = SYSDATE
FROM (
      SELECT fv.entity_id, spf.sub_panel_id, fv.value
       FROM panel.fvalues3 fv
       INNER JOIN panel.Sub_panel_features spf ON spf.feature_id = fv.feature_id
       INNER JOIN wh.subpanel_feature_target ft ON ( spf.feature_order = ft.feature_order AND ft.feature_order = 110 )
      WHERE fv.wh_datemod >= CURRENT_DATE - INTERVAL '10 DAY'
       ) src
WHERE wh.dim_subpanel_membership.entity_id = src.entity_id
AND wh.dim_subpanel_membership.sub_panel_id = src.sub_panel_id
AND
 (
  wh.dim_subpanel_membership.privacy_terms_consent_date IS NULL
  OR
  wh.dim_subpanel_membership.privacy_terms_consent_date <> src.value
 )
;



--added 2013-04-03 per instructions from Vertica Support (Case #00016628)
SELECT set_optimizer_directives('DisableDeleteOpt=true');


--Respondent Points Fraction
--added 2013-01-29
UPDATE wh.dim_subpanel_membership
SET
	  respondent_points_fraction = src.value
	, wh_datemod = SYSDATE
FROM (
      SELECT fv.entity_id, spf.sub_panel_id, fv.value
       FROM panel.fvalues2 fv
       INNER JOIN panel.Sub_panel_features spf ON spf.feature_id = fv.feature_id
       INNER JOIN wh.subpanel_feature_target ft ON ( spf.feature_order = ft.feature_order AND ft.feature_order = 117 )
       WHERE fv.wh_datemod >= CURRENT_DATE - INTERVAL '10 DAY'
       ) src
WHERE wh.dim_subpanel_membership.entity_id = src.entity_id
AND wh.dim_subpanel_membership.sub_panel_id = src.sub_panel_id
AND
 (
  wh.dim_subpanel_membership.respondent_points_fraction IS NULL
  OR
  wh.dim_subpanel_membership.respondent_points_fraction <> src.value
 )
;



 --added 2013-04-03 per instructions from Vertica Support (Case #00016628)
SELECT set_optimizer_directives('DisableDeleteOpt=false');


--Respondent Consent
--added 2013-03-25
UPDATE wh.dim_subpanel_membership
 SET
     respondent_consent_type = src.respondent_consent_type
   , respondent_consent_date = src.respondent_consent_date
   , wh_datemod = SYSDATE
 FROM
 (
 SELECT
  fv.entity_id,
  spf.sub_panel_id,
  fv.value AS respondent_consent_type,
  fv.modified_at AS respondent_consent_date
 FROM panel.fvalues1 fv
 INNER JOIN panel.Sub_panel_features spf ON spf.feature_id = fv.feature_id
 INNER JOIN wh.subpanel_feature_target ft ON ( spf.feature_order = ft.feature_order AND ft.feature_order = 129 )
 WHERE fv.wh_datemod >= CURRENT_DATE - INTERVAL '10 DAY'
 ) src
 WHERE wh.dim_subpanel_membership.entity_id = src.entity_id
 AND wh.dim_subpanel_membership.sub_panel_id = src.sub_panel_id
 AND
 (
 wh.dim_subpanel_membership.respondent_consent_type IS NULL
 OR
 wh.dim_subpanel_membership.respondent_consent_type <> src.respondent_consent_type
 );



--------------------------------------------
--  dim_respondent_source_id
--------------------------------------------

--Epanel/surveyspot
UPDATE wh.dim_subpanel_membership
SET
	  dim_respondent_source_id = src.dim_respondent_source_id
	, wh_datemod = SYSDATE
FROM
 ( SELECT
    e.entity_id,
    e.sub_panel_id,
    rs.dim_respondent_source_id
  FROM  recruitment.epanel_legacy_recruitment_cost e
  INNER JOIN wh.dim_respondent_source rs ON e.epanel_promotionunitid = rs.epanel_promotionunitid
 ) src
WHERE wh.dim_subpanel_membership.entity_id = src.entity_id
AND wh.dim_subpanel_membership.sub_panel_id = src.sub_panel_id
AND  wh.dim_subpanel_membership.dim_respondent_source_id IS NULL
AND
	(
		wh.dim_subpanel_membership.campaignid IN ( 2383,2514,2515,5616 ) --Spot Migration
 		OR
 		wh.dim_subpanel_membership.campaignid IS NULL
 	)
;



--OO Goldrush
UPDATE wh.dim_subpanel_membership
SET
		  dim_respondent_source_id = src.dim_respondent_source_id
		, wh_datemod = SYSDATE
FROM
 ( SELECT
    o.entity_id,
    o.sub_panel_id,
    rs.dim_respondent_source_id
  FROM  recruitment.oo_legacy_recruitment_detail o
  INNER JOIN wh.dim_respondent_source rs ON o.affiliate_campaign_id = rs.goldrush_affiliatecampaignid
 ) src
WHERE wh.dim_subpanel_membership.entity_id = src.entity_id
AND wh.dim_subpanel_membership.sub_panel_id = src.sub_panel_id
AND (
			wh.dim_subpanel_membership.campaignid IN ( 5694, 5770  ) --Opinion Outpost Migration
      OR
      wh.dim_subpanel_membership.campaignid IS NULL
      )
AND wh.dim_subpanel_membership.dim_respondent_source_id IS NULL
;



--SELECT 'dim_respondent_source_id -- OO Goldrush' as step_name, COUNT(*) AS number_of_constraint_violations FROM ( SELECT analyze_constraints('wh.dim_subpanel_membership') ) x;

--HasOffers Web recruitment
UPDATE wh.dim_subpanel_membership
SET
		  dim_respondent_source_id = src.dim_respondent_source_id
		, wh_datemod = SYSDATE
FROM
 (
 SELECT
  a.entity_id,
  a.sub_panel_id,
  rs.dim_respondent_source_id
 FROM
  (
  SELECT
   f4.entity_id,
   spf.sub_panel_id,
   SPLIT_PART( f4.value, '_', 1 )||'_'|| SPLIT_PART( f4.value, '_', 2 ) AS offerid_affiliateid
  FROM panel.sub_panel_features spf
  INNER JOIN panel.fvalues4 f4 ON ( spf.feature_id = f4.feature_id AND spf.feature_order = 2 )
  WHERE LENGTH(f4.value) > 1
  AND f4.modified_at > TO_DATE( '2011-06-01', 'YYYY-MM-DD')
  ) a
 INNER JOIN wh.dim_respondent_source rs ON a.offerid_affiliateid = rs.hasoffers_offerid_affiliateid
) src
WHERE wh.dim_subpanel_membership.entity_id = src.entity_id
AND wh.dim_subpanel_membership.sub_panel_id = src.sub_panel_id
AND wh.dim_subpanel_membership.dim_respondent_source_id IS NULL
;



--HasOffers Mobile recruitment (QuickThoughts Panel)

UPDATE wh.dim_subpanel_membership
SET
	  dim_respondent_source_id = rs.dim_respondent_source_id
	, wh_datemod = SYSDATE
FROM wh.dim_respondent_source rs
WHERE wh.dim_subpanel_membership.respondent_source = rs.respondent_source_name
AND rs.sourcesystemid = 14 ----HasOffers Mobile recruitment
AND wh.dim_subpanel_membership.sub_panel_id = 38 --QuickThoughts
AND wh.dim_subpanel_membership.dim_respondent_source_id IS NULL
;


--Recruit Management Tool (RMT)
UPDATE wh.dim_subpanel_membership
SET
		  dim_respondent_source_id = rs.dim_respondent_source_id
		, wh_datemod = SYSDATE
FROM wh.dim_respondent_source rs
WHERE wh.dim_subpanel_membership.campaignid = rs.rmt_campaignid
AND wh.dim_subpanel_membership.campaignid NOT IN ( 2383,2514,2515,5616,5694,5770 )  --exclude Opinion Outpost & SurveySpot Migration
AND wh.dim_subpanel_membership.dim_respondent_source_id IS NULL
;


--log process end
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_dim_subpanel_membership', 'end', SYSDATE );
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_dim_subpanel_membership', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.dim_subpanel_membership;
