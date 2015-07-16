--update dim_subpanel

--log process start
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_dim_sub_panel', 'start', SYSDATE );
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id )
SELECT 'update_dim_sub_panel', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.dim_sub_panel;

--insert records for new subpanels
INSERT INTO wh.dim_sub_panel ( sub_panel_id )
SELECT sub_panel_id
FROM panel.sub_panels
WHERE sub_panel_id NOT IN ( SELECT sub_panel_id FROM wh.dim_sub_panel )
;



--update name and description to current values
UPDATE wh.dim_sub_panel
	SET
		  name = src.name
		, description = src.description
		,	wh_datemod = SYSDATE
FROM  panel.sub_panels src
WHERE wh.dim_sub_panel.sub_panel_id = src.sub_panel_id
AND (
			COALESCE( wh.dim_sub_panel.name, 'X' ) <> COALESCE( src.name, 'X' )
      OR
      COALESCE( wh.dim_sub_panel.description, 'X' ) <> COALESCE( src.description, 'X' )
     )
;



--set subpanel type based on extended attribute 5
UPDATE  wh.dim_sub_panel
SET
   sub_panel_type_id = src.sub_panel_type_id
 , sub_panel_type_name = src.sub_panel_type_name
 , sub_panel_type_description = src.sub_panel_type_description
 ,wh_datemod = SYSDATE
FROM
(
SELECT
	sp.sub_panel_id,
	ev.name as sub_panel_type_name,
	eav1.value as sub_panel_type_id,
	CASE
		WHEN eav1.value = 1 THEN 'Proprietary'
		WHEN eav1.value = 2 THEN 'Affiliate'
	ELSE NULL
  END AS sub_panel_type_description
FROM panel.sub_panels sp
INNER JOIN panel.Extended_attribute_values_1 eav1 ON
	(
	  eav1.object_id = sp.sub_panel_id AND
	  eav1.object_type_id  = 230 /* sub-panel */ AND
	  eav1.extended_attribute_id = 5 /* sub-panel type */
	 )
INNER JOIN panel.Extended_attributes ea ON eav1.extended_attribute_id = ea.extended_attribute_id
INNER JOIN panel.enum_values ev ON
 (
  ev.enum_type_id = ea.enum_type_id AND
  ev.enum_value_id = eav1.value
 )
WHERE sp.sub_panel_id not in ( 0 ) --exclude external partner subpanel
) src
WHERE  wh.dim_sub_panel.sub_panel_id = src.sub_panel_id
AND
	( wh.dim_sub_panel.sub_panel_type_id IS NULL
	  OR
	  wh.dim_sub_panel.sub_panel_type_id <> src.sub_panel_type_id
	 )
;



--set channel based on subpanel type (for Proprietary and Affiliate channels)
UPDATE wh.dim_sub_panel
SET dim_channel_id = CASE
											WHEN sub_panel_type_id = 1 THEN 1 --Proprietary
											WHEN sub_panel_type_id = 2 THEN 3 --Affiliate
										ELSE NULL
										END
										,	wh_datemod = SYSDATE
WHERE  sub_panel_type_id IN ( 1, 2 )
AND sub_panel_id NOT IN ( 0 ) --exclude external partner subpanel
AND
(
	dim_channel_id IS NULL
      OR
      dim_channel_id <> CASE
											WHEN sub_panel_type_id = 1 THEN 1 --Proprietary
											WHEN sub_panel_type_id = 2 THEN 3 --Affiliate
										ELSE NULL
										END
) ;

--Set channel based on extended attribute 161 (for UMAS channel)
UPDATE wh.dim_sub_panel
SET
    dim_channel_id = 4
  , wh_datemod = SYSDATE
where sub_panel_id IN
(
SELECT
	sp.sub_panel_id
FROM panel.sub_panels sp
INNER JOIN panel.Extended_attribute_values_1 eav1 ON
	(
	  eav1.object_id = sp.sub_panel_id AND
	  eav1.object_type_id  = 230 /* sub-panel */ AND
	  eav1.extended_attribute_id = 161	 /* UMA Sub-panel type */
	 )
WHERE eav1.value > 0 --UMAS sub_panels
AND sp.sub_panel_id NOT IN ( 0 ) --exclude external partner subpanel
)
 ;

 --pricing metrics enabled flag
--added 2013-03-25
UPDATE  wh.dim_sub_panel
SET
   pricing_metrics_enabled = src.pricing_metrics_enabled
 , wh_datemod = SYSDATE
FROM
(
SELECT
	sp.sub_panel_id,
	eav1.value as pricing_metrics_enabled
FROM panel.sub_panels sp
INNER JOIN panel.Extended_attribute_values_1 eav1 ON
	(
	  eav1.object_id = sp.sub_panel_id AND
	  eav1.object_type_id  = 230 /* sub-panel */ AND
	  eav1.extended_attribute_id = 194 /* Pricing Metrics Enabled */
	 )
INNER JOIN panel.Extended_attributes ea ON eav1.extended_attribute_id = ea.extended_attribute_id
) src
WHERE  wh.dim_sub_panel.sub_panel_id = src.sub_panel_id
;



--log process end
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_dim_sub_panel', 'end', SYSDATE );
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id )
SELECT 'update_dim_sub_panel', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.dim_sub_panel;