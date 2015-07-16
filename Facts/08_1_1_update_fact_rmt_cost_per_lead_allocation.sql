--log event start
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'fact_rmt_cost_per_lead_allocation', 'start', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'fact_rmt_cost_per_lead_allocation', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.fact_rmt_cost_per_lead_allocation;

CREATE LOCAL TEMPORARY TABLE fact_rmt_cost_per_lead_allocation_TEMP
(
dim_respondent_source_id INT NOT NULL,
month_id            INT NOT NULL,
dim_country_id      INT NOT NULL,
sub_panel_id        INT NOT NULL,
dim_currency_id     INT NOT NULL,
unit_cost_nominal   NUMERIC(18,4),
total_cost          NUMERIC(18,4),
nPayment_Events     INT,
nJoins              INT,
wh_datemod          TIMESTAMP DEFAULT SYSDATE
)
ON COMMIT PRESERVE ROWS
;

TRUNCATE TABLE fact_rmt_cost_per_lead_allocation_TEMP;

INSERT INTO fact_rmt_cost_per_lead_allocation_TEMP
(
 dim_respondent_source_id,
 month_id,
 dim_country_id,
 sub_panel_id,
 dim_currency_id,
 unit_cost_nominal,
 total_cost,
 nPayment_Events,
 nJoins
)
SELECT
 rs.dim_respondent_source_id,
 a.month_id,
 a.dim_country_id,
 a.sub_panel_id,
 a.dim_currency_id,
 a.unit_cost_nominal,
 a.total_cost,
 a.nPayment_Events,
 a.nJoins
FROM
( 
SELECT
 i.campaignid,
 m.month_id,
 COALESCE( ctry.dim_country_id, 243 ) AS dim_country_id,
 cmpn.subpanelid AS sub_panel_id,
 crncy.dim_currency_id,
 cmpn.price as unit_cost_nominal,
 SUM( cmpn.price ) AS total_cost,
 COUNT(*)  AS nPayment_Events,
 SUM( CASE WHEN spm.membershipstatusid > 1 THEN 1 ELSE 0 END ) AS nJoins
FROM intake.intake i
INNER JOIN reporting.RMT_Campaign cmpn ON i.campaignid = cmpn.campaignId
INNER JOIN wh.dim_currency crncy ON cmpn.CurrencyID = crncy.rmt_currency_id
LEFT OUTER JOIN wh.dim_respondent r ON i.entity_id = r.entity_id
LEFT OUTER JOIN wh.dim_subpanel_membership spm 
 ON ( i.entity_id = spm.entity_id AND
      cmpn.subPanelId = spm.sub_panel_id
      )
LEFT OUTER JOIN wh.dim_country ctry ON r.country_enum_value_id = ctry.country_enum_value_id
INNER JOIN wh.dim_month m ON m.yearmo =  TO_NUMBER(TO_CHAR( i.intake_date, 'YYYYMM') )
WHERE i.intake_date >= ADD_MONTHS( DATE_TRUNC('MONTH', SYSDATE ), -1 )
AND ( 
       cmpn.typeId = 6 /* gross leads */
       OR
       ( cmpn.typeId = 1 AND i.intake_status_id  =  1 /* net leads */ ) 
        )
GROUP BY
 i.campaignid,
 m.month_id,
 COALESCE( ctry.dim_country_id, 243 ),
 cmpn.subpanelid,
 crncy.dim_currency_id,
 cmpn.price 
) a
INNER JOIN wh.dim_respondent_source rs ON a.campaignid = rs.rmt_campaignid
;




--INSERT HISTORICAL ROWS INTO TEMP TABLE
INSERT INTO fact_rmt_cost_per_lead_allocation_TEMP
(
 dim_respondent_source_id,
 month_id,
 dim_country_id,
 sub_panel_id,
 dim_currency_id,
 unit_cost_nominal,
 total_cost,
 nPayment_Events,
 nJoins,
 wh_datemod
)
SELECT
dim_respondent_source_id,
 month_id,
 dim_country_id,
 sub_panel_id,
 dim_currency_id,
 unit_cost_nominal,
 total_cost,
 nPayment_Events,
 nJoins,
 wh_datemod
FROM wh.fact_rmt_cost_per_lead_allocation
WHERE month_id NOT IN ( SELECT DISTINCT month_id FROM fact_rmt_cost_per_lead_allocation_TEMP );



--truncate the target table
TRUNCATE TABLE wh.fact_rmt_cost_per_lead_allocation;

--insert rows from temp table into target
INSERT INTO wh.fact_rmt_cost_per_lead_allocation
(
 dim_respondent_source_id,
 month_id,
 dim_country_id,
 sub_panel_id,
 dim_currency_id,
 unit_cost_nominal,
 total_cost,
 nPayment_Events,
 nJoins,
 wh_datemod
)
SELECT
 dim_respondent_source_id,
 month_id,
 dim_country_id,
 sub_panel_id,
 dim_currency_id,
 unit_cost_nominal,
 total_cost,
 nPayment_Events,
 nJoins,
 wh_datemod
FROM fact_rmt_cost_per_lead_allocation_TEMP
;

--log event end
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'fact_rmt_cost_per_lead_allocation', 'end', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'fact_rmt_cost_per_lead_allocation', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.fact_rmt_cost_per_lead_allocation;