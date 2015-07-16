DROP TABLE IF EXISTS work.dim_subpanel_membership_dupes CASCADE;

CREATE TABLE work.dim_subpanel_membership_dupes
AS
SELECT * FROM
(
SELECT
   entity_id
 , sub_panel_id
 , membershipstatusid
 , date_membership_status_mod
 , date_membership_started
 , date_membership_ended
 , campaignid
 , is_active
 , external_system_member_id
 , wh_datemod
 , respondent_source
 , dim_respondent_source_id
 , privacy_terms_consent_date
 , respondent_points_fraction
 , respondent_consent_type
 , respondent_consent_date
 , COUNT(*) OVER ( PARTITION BY entity_id, sub_panel_id ) AS number_of_records
 , ROW_NUMBER() OVER ( PARTITION BY entity_id, sub_panel_id ORDER BY wh_datemod DESC ) AS rnum
FROM wh.dim_subpanel_membership
) a
WHERE a.number_of_records > 1
AND a.rnum = 1
;

DELETE FROM wh.dim_subpanel_membership
   WHERE (entity_id, sub_panel_id ) IN
     (SELECT entity_id, sub_panel_id  FROM work.dim_subpanel_membership_dupes) 
     ;

INSERT /*+ DIRECT */ INTO wh.dim_subpanel_membership
 (
  entity_id
 , sub_panel_id
 , membershipstatusid
 , date_membership_status_mod
 , date_membership_started
 , date_membership_ended
 , campaignid
 , is_active
 , external_system_member_id
 , wh_datemod
 , respondent_source
 , dim_respondent_source_id
 , privacy_terms_consent_date
 , respondent_points_fraction
 , respondent_consent_type
 , respondent_consent_date
 )
SELECT 
 entity_id
 , sub_panel_id
 , membershipstatusid
 , date_membership_status_mod
 , date_membership_started
 , date_membership_ended
 , campaignid
 , is_active
 , external_system_member_id
 , wh_datemod
 , respondent_source
 , dim_respondent_source_id
 , privacy_terms_consent_date
 , respondent_points_fraction
 , respondent_consent_type
 , respondent_consent_date
FROM work.dim_subpanel_membership_dupes
;