--Update wh.dim_currency

--log process start
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_dim_currency', 'start', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_dim_currency', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.dim_currency;


CREATE LOCAL TEMPORARY TABLE dim_currency_temp
 (
   ssinebu_currency_id INT,
   currency_enum_value_id INT,
   rmt_currency_id INT,
   currency_iso_code VARCHAR(3), 
   currency_name VARCHAR(100)
  )
ON COMMIT PRESERVE ROWS 
UNSEGMENTED ALL NODES
;


--insert rows for all currencies into temp table
INSERT INTO dim_currency_temp
(
 ssinebu_currency_id,
 currency_enum_value_id,
 rmt_currency_id,
 currency_iso_code, 
 currency_name
)
SELECT 
 a.ssinebu_currency_id,
 a.currency_enum_value_id,
 rmt.id AS rmt_currency_id,
 COALESCE( a.currency_iso_code, rmt.symbol ) AS currency_iso_code, 
 COALESCE( a.currency_name, rmt.name ) AS currency_name
FROM 
(
SELECT 
 sn.currency_id AS ssinebu_currency_id, 
 COALESCE( sn.currency_enum_value_id, ev.enum_value_id ) AS currency_enum_value_id,
 sn.iso_code AS currency_iso_code, 
 COALESCE( sn.currency_name,  ev.name ) AS currency_name
FROM  ssinebu.currency sn 
FULL OUTER JOIN panel.Enum_values ev ON ( sn.currency_enum_value_id = ev.enum_value_id  )
WHERE ( ev.enum_type_id = 2857 OR ev.enum_type_id IS NULL )
) a
FULL OUTER JOIN reporting.currency rmt ON a.currency_iso_code = rmt.symbol
;

--delete rows from temp table that already exist in target table
DELETE FROM dim_currency_temp
WHERE 
 ( ssinebu_currency_id IN ( SELECT ssinebu_currency_id FROM wh.dim_currency )
   OR
   currency_enum_value_id IN ( SELECT currency_enum_value_id FROM wh.dim_currency )
   OR 
   rmt_currency_id IN ( SELECT rmt_currency_id FROM wh.dim_currency )
   OR
   currency_iso_code IN ( SELECT currency_iso_code FROM wh.dim_currency )
  )
;

--insert new rows into target
INSERT INTO wh.dim_currency
(
 ssinebu_currency_id,
 currency_enum_value_id,
 rmt_currency_id,
 currency_iso_code, 
 currency_name
)
SELECT
 ssinebu_currency_id,
 currency_enum_value_id,
 rmt_currency_id,
 currency_iso_code, 
 currency_name
FROM dim_currency_temp
;
 
--log process end
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_dim_currency', 'end', SYSDATE );   
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_dim_currency', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.dim_currency;