
--event start ROW
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_hasoffers_respondent_cost', 'start', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_hasoffers_respondent_cost', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM recruitment.hasoffers_respondent_cost;
            
DROP TABLE IF EXISTS hasoffers_respondent_cost_delta;

CREATE LOCAL TEMPORARY TABLE hasoffers_respondent_cost_delta
ON COMMIT PRESERVE ROWS 
AS
SELECT 
	  entity_id	
		, sub_panel_id
		, dim_respondent_source_id
		, dim_currency_id
		, currency_iso_code
		, respondent_source_modified_at
		, respondent_source_value
		, offerid
		, affiliateid
		, offerid_affiliateid
		, affiliate_sub_id
		, transaction_id
		, cost
FROM recruitment.v_hasoffers_respondent_cost
WHERE respondent_source_modified_at > CURRENT_DATE - INTERVAL '10 DAYS'
;

		
		
MERGE INTO recruitment.hasoffers_respondent_cost tgt
USING hasoffers_respondent_cost_delta src
 ON 
 	(
 	 tgt.entity_id = src.entity_id AND 
 	 tgt.sub_panel_id = src.sub_panel_id
 	 )
WHEN MATCHED THEN UPDATE
	SET	
		  dim_respondent_source_id        = src.dim_respondent_source_id           
		, dim_currency_id                 = src.dim_currency_id              
		, currency_iso_code               = src.currency_iso_code            
		, respondent_source_modified_at   = src.respondent_source_modified_at
		, respondent_source_value         = src.respondent_source_value      
		, offerid                         = src.offerid                      
		, affiliateid                     = src.affiliateid                  
		, offerid_affiliateid             = src.offerid_affiliateid          
		, affiliate_sub_id                = src.affiliate_sub_id             
		, transaction_id                  = src.transaction_id               
		, cost                            = src.cost        
		, wh_datemod                      = SYSDATE                 
WHEN NOT MATCHED THEN INSERT
	(
		  entity_id	
		, sub_panel_id
		, dim_respondent_source_id
		, dim_currency_id
		, currency_iso_code
		, respondent_source_modified_at
		, respondent_source_value
		, offerid
		, affiliateid
		, offerid_affiliateid
		, affiliate_sub_id
		, transaction_id
		, cost
	)
	VALUES
		(
		  src.entity_id	
		, src.sub_panel_id
		, src.dim_respondent_source_id
		, src.dim_currency_id
		, src.currency_iso_code
		, src.respondent_source_modified_at
		, src.respondent_source_value
		, src.offerid
		, src.affiliateid
		, src.offerid_affiliateid
		, src.affiliate_sub_id
		, src.transaction_id
		, src.cost
	);





--event end row 
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_hasoffers_respondent_cost', 'end', SYSDATE );     
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_hasoffers_respondent_cost', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM recruitment.hasoffers_respondent_cost;