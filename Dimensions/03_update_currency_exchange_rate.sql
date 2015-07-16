--put the current set of currency exchange rates into a table

--log process start
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'upd_currency_exchange_rate', 'start', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'upd_currency_exchange_rate', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.currency_exchange_rate;


TRUNCATE TABLE wh.currency_exchange_rate;

INSERT INTO wh.currency_exchange_rate
( 
	date_effective, 
	source_currency_iso,
	source_dim_currency_id, 
	dest_currency_iso, 
	dest_dim_currency_id,
	source_to_dest_exchange_rate 
)
SELECT 
	date_effective, 
	source_currency_iso, 
	source_dim_currency_id,
	dest_currency_iso, 
	dest_dim_currency_id,
	source_to_dest_exchange_rate 
FROM wh.v_currency_exchange_Rate
;


--log process end
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'upd_currency_exchange_rate', 'end', SYSDATE );   
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'upd_currency_exchange_rate', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.currency_exchange_rate;