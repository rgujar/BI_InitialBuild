--update_dim_business_line

--log process start
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_dim_business_line', 'start', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_dim_business_line', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.dim_business_line;

INSERT INTO wh.dim_business_line 
 ( 
   bos_ssi_businesslineid
 , name 
 )
SELECT 
	  Ssi_businesslineId
	, Ssi_name
FROM bos.dw_crm_businessline
WHERE Ssi_businesslineId NOT IN
	 ( SELECT bos_ssi_businesslineid 
	   FROM wh.dim_business_line )
;

--log process end
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_dim_business_line', 'end', SYSDATE );  
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_dim_business_line', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.dim_business_line;