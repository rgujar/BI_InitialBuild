--update wh.membership_status_history

--event start ROW
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_membership_status_history', 'start', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_membership_status_history', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.membership_status_history;  


DROP TABLE IF EXISTS membership_status_history_temp CASCADE;
         
CREATE LOCAL TEMPORARY TABLE membership_status_history_temp
(
entity_id INT,
sub_panel_id INT,
combined_membershipstatusid INT,
change_date DATETIME,
prev_combined_membershipstatusid INT,
status_change_type_id INT,
yearmo INT,
wh_datemod TIMESTAMP DEFAULT SYSDATE
)
ON COMMIT PRESERVE ROWS
UNSEGMENTED ALL NODES
;


INSERT INTO membership_status_history_temp
(
entity_id,
sub_panel_id,
combined_membershipstatusid,
change_date,
prev_combined_membershipstatusid,
status_change_type_id
)
SELECT
dsm.entity_id,
dsm.sub_panel_id,
rl.combined_membershipstatusid,
CASE 
	WHEN r.date_membership_status_mod >= l.change_date+1 AND r.date_membership_status_mod >= dsm.date_membership_status_mod THEN r.date_membership_status_mod
	WHEN dsm.date_membership_status_mod >= l.change_date+1 AND dsm.date_membership_status_mod >= r.date_membership_status_mod THEN dsm.date_membership_status_mod
ELSE l.change_date+1 
END AS change_date,
l.combined_membershipstatusid,
CASE 
  WHEN rl.combined_membershipstatusid in (9,13,14,20) THEN 10
  WHEN rl.combined_membershipstatusid=4 AND l.combined_membershipstatusid>4 THEN 1
  WHEN rl.combined_membershipstatusid=4 AND l.combined_membershipstatusid<4 THEN 2
  WHEN rl.combined_membershipstatusid>4 AND l.combined_membershipstatusid=4 THEN 3
  WHEN rl.combined_membershipstatusid>4 AND l.combined_membershipstatusid>4 THEN 4
  WHEN rl.combined_membershipstatusid>4 AND l.combined_membershipstatusid<4 THEN 5
  WHEN rl.combined_membershipstatusid<4 AND l.combined_membershipstatusid=15 THEN 6
  WHEN rl.combined_membershipstatusid<4 AND l.combined_membershipstatusid<4 THEN 7
  WHEN rl.combined_membershipstatusid<4 AND l.combined_membershipstatusid=4 THEN 8
  WHEN rl.combined_membershipstatusid<4 AND l.combined_membershipstatusid>4 THEN 9
ELSE 11
END AS status_change_type_id
FROM wh.dim_sub_panel sp
INNER JOIN wh.dim_channel chnl ON sp.dim_channel_id = chnl.dim_channel_id
INNER JOIN wh.dim_subpanel_membership dsm on sp.sub_panel_id=dsm.sub_panel_id
INNER JOIN wh.dim_respondent r on dsm.entity_id=r.entity_id
INNER JOIN wh.membershipstatus_rule rl on r.membershipstatusid=rl.respondent_membershipstatusid and dsm.membershipstatusid=rl.subpanel_membershipstatusid
INNER JOIN wh.v_last_membership_status l on dsm.entity_id=l.entity_id and dsm.sub_panel_id=l.sub_panel_id
WHERE chnl.channel_name NOT IN ('Partner', 'UMAS', 'Unknown')
AND l.combined_membershipstatusid<>rl.combined_membershipstatusid
;

UPDATE membership_status_history_temp
	SET yearmo = TO_NUMBER(TO_CHAR( change_date , 'YYYYMM' ))
;




DROP TABLE IF EXISTS subpanel_membership_new_temp CASCADE;

CREATE LOCAL TEMPORARY TABLE subpanel_membership_new_temp
(
entity_id INT,
sub_panel_id INT,
membershipstatusid INT,
date_membership_status_mod DATETIME
)
ON COMMIT PRESERVE ROWS
UNSEGMENTED ALL NODES
;


INSERT INTO subpanel_membership_new_temp
(
entity_id,
sub_panel_id,
membershipstatusid,
date_membership_status_mod
)
SELECT 
	  dsm.entity_id
	, dsm.sub_panel_id
	, dsm.membershipstatusid
	, dsm.date_membership_status_mod 
FROM wh.dim_sub_panel sp
INNER JOIN wh.dim_channel chnl ON sp.dim_channel_id = chnl.dim_channel_id
INNER JOIN wh.dim_subpanel_membership dsm on sp.sub_panel_id=dsm.sub_panel_id
WHERE chnl.channel_name NOT IN ( 'Partner', 'UMAS', 'Unknown' ) 
AND ( dsm.entity_id, dsm.sub_panel_id ) NOT IN 
	( 
		SELECT entity_id, sub_panel_id 
		FROM wh.v_last_membership_status 
	) 
AND dsm.membershipstatusid IS NOT NULL
;



INSERT INTO membership_status_history_temp
(
entity_id,
sub_panel_id,
combined_membershipstatusid,
change_date,
status_change_type_id,
yearmo,
wh_datemod
)
SELECT 
dsm.entity_id,
dsm.sub_panel_id,
rl.combined_membershipstatusid,
CASE WHEN r.date_membership_status_mod>=dsm.date_membership_status_mod THEN r.date_membership_status_mod ELSE dsm.date_membership_status_mod END AS change_date,
CASE 
  WHEN rl.combined_membershipstatusid in (9,13,14,20) THEN 10
  WHEN rl.combined_membershipstatusid=4 THEN 2
  WHEN rl.combined_membershipstatusid>4 THEN 5
  WHEN rl.combined_membershipstatusid<4 THEN 6
ELSE 11 
END AS status_change_type_id, 
TO_NUMBER(TO_CHAR(case when r.date_membership_status_mod>=dsm.date_membership_status_mod then r.date_membership_status_mod else dsm.date_membership_status_mod end, 'YYYYMM')) as yearmo,
SYSDATE
FROM subpanel_membership_new_temp dsm
INNER JOIN wh.dim_respondent r on dsm.entity_id=r.entity_id
INNER JOIN wh.membershipstatus_rule rl on r.membershipstatusid=rl.respondent_membershipstatusid and dsm.membershipstatusid=rl.subpanel_membershipstatusid;



INSERT INTO wh.membership_status_history
(
entity_id,
sub_panel_id,
combined_membershipstatusid,
change_date,
prev_combined_membershipstatusid,
status_change_type_id,
yearmo,
wh_datemod 
)
SELECT 
	entity_id,
	sub_panel_id,
	combined_membershipstatusid,
	change_date,
	prev_combined_membershipstatusid,
	status_change_type_id,
	yearmo,
	wh_datemod 
FROM membership_status_history_temp
 ;



--event end row 
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_membership_status_history', 'end', SYSDATE );     
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_membership_status_history', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.membership_status_history; 