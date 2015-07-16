--log event start
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_dim_subpanel_member_cookie', 'start', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_dim_subpanel_member_cookie', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.dim_subpanel_member_cookie;
--update dim_subpanel_member_cookie
DROP TABLE IF EXISTS dim_subpanel_member_cookie_temp;

CREATE LOCAL TEMPORARY TABLE dim_subpanel_member_cookie_temp
(
	 entity_id INT NOT NULL
 , sub_panel_id INT NOT NULL ENCODING RLE
 , feature_id INT NOT NULL ENCODING RLE
 , enum_type_id INT NOT NULL  ENCODING RLE
 , enum_value_id INT NOT NULL ENCODING RLE
 , modified_at TIMESTAMP
 , yearmo INT NOT NULL ENCODING RLE
 , wh_datemod TIMESTAMP DEFAULT CURRENT_TIMESTAMP
 , CONSTRAINT subpanel_member_cookie_pk PRIMARY KEY (entity_id, sub_panel_id, feature_id, enum_type_id, enum_value_id )
)
ON COMMIT PRESERVE ROWS
;


INSERT INTO dim_subpanel_member_cookie_temp
(
   entity_id
 , sub_panel_id
 , feature_id
 , enum_type_id
 , enum_value_id
 , modified_at
 , yearmo
)
SELECT 
   f1.entity_id
 , spf.sub_panel_id
 , f.feature_id
 , ev.enum_type_id
 , ev.enum_value_id
 , f1.modified_at
 , f1.yearmo
FROM panel.Features f
INNER JOIN panel.Sub_panel_features spf ON f.feature_id = spf.feature_id
INNER JOIN panel.FValues1 f1 ON f.feature_id = f1.feature_id
INNER JOIN panel.mvalues mv ON f1.value = mv.mvalue_id 
INNER JOIN panel.enum_values ev ON mv.enum_value_id = ev.enum_value_id
WHERE spf.feature_order = 119 --Partner Tracking Cookies
AND ev.enum_type_id = 8366
AND f1.yearmo BETWEEN 
	TO_NUMBER( TO_CHAR( ADD_MONTHS( CURRENT_TIMESTAMP, - 1 ), 'YYYYMM' ) ) --previous month
	AND 
	TO_NUMBER( TO_CHAR( CURRENT_TIMESTAMP, 'YYYYMM' ) ) --current month
;


	


MERGE INTO wh.dim_subpanel_member_cookie tgt
USING dim_subpanel_member_cookie_temp src
ON 
	( 
		src.entity_id        = tgt.entity_id AND
		src.sub_panel_id     = tgt.sub_panel_id  AND 
		src.feature_id       = tgt.feature_id AND
		src.enum_type_id     = tgt.enum_type_id  AND 
		src.enum_value_id    = tgt.enum_value_id  
	)
WHEN MATCHED THEN UPDATE
	SET	
		  modified_at = src.modified_at
		, wh_datemod = SYSDATE
WHEN NOT MATCHED THEN INSERT
(
   entity_id
 , sub_panel_id
 , feature_id
 , enum_type_id
 , enum_value_id
 , modified_at
 , yearmo
)
VALUES
(
   src.entity_id
 , src.sub_panel_id
 , src.feature_id
 , src.enum_type_id
 , src.enum_value_id
 , src.modified_at
 , src.yearmo   
)
;

--log event end
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES ( 'update_dim_subpanel_member_cookie', 'end', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_dim_subpanel_member_cookie', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.dim_subpanel_member_cookie;