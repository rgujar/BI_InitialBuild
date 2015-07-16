--log event start
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'fact_recruitment_cost', 'start', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'fact_recruitment_cost', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.fact_recruitment_cost;

DROP TABLE IF EXISTS fact_recruitment_cost_temp;

CREATE LOCAL TEMPORARY TABLE fact_recruitment_cost_temp
(
  dim_respondent_source_id  INT NOT NULL,
  month_id                  INT NOT NULL,
  dim_country_id            INT NOT NULL,
  sub_panel_id              INT NOT NULL,
  dim_currency_id           INT NOT NULL,
  total_recruit_cost        NUMERIC(18,4),
  nPayment_Events           INT,
  nLeads                    INT,
  nJoins                    INT,
  nM17                      INT,
  nM1824                    INT,
  nM2529                    INT,
  nM3039                    INT,
  nM4049                    INT,
  nM5064                    INT,
  nM65                      INT,
  nF17                      INT,
  nF1824                    INT,
  nF2529                    INT,
  nF3039                    INT,
  nF4049                    INT,
  nF5064                    INT,
  nF65                      INT,
  wh_datemod                TIMESTAMP DEFAULT SYSDATE,
  CONSTRAINT fact_recruitment_cost_pk PRIMARY KEY ( dim_respondent_source_id, month_id, dim_country_id, sub_panel_id )
)
ON COMMIT PRESERVE ROWS
UNSEGMENTED ALL NODES
;


----------------------------------------
--insert delta rows into temp table
-----------------------------------------

INSERT INTO fact_recruitment_cost_temp
 (
  dim_respondent_source_id,
  month_id,
  dim_country_id,
  sub_panel_id,
  dim_currency_id,
  total_recruit_cost,
  nPayment_Events,
  nLeads,
  nJoins,
  nM17,
  nM1824,
  nM2529,
  nM3039,
  nM4049,
  nM5064,
  nM65,
  nF17,
  nF1824,
  nF2529,
  nF3039,
  nF4049,
  nF5064,
  nF65
 )
SELECT
 ifnull(r.dim_respondent_source_id,0) as dim_respondent_source_id,
 r.month_id,
 r.dim_country_id,
 r.sub_panel_id,
 ifnull(r.dim_currency_id,1030) as dim_currency_id,
 SUM(r.cost) AS total_recruit_cost,
 SUM(case when r.cost>0 then 1 end) as nPayment_Events,
 COUNT(*) as nLeads,
 SUM(r.is_join) as nJoins,
 sum(case when dr.gender_code_iso=1 and AGE_IN_YEARS(dr.date_born)<18 and r.is_join=1 then 1 end) as nM17,
 sum(case when dr.gender_code_iso=1 and AGE_IN_YEARS(dr.date_born) between 18 and 24 and r.is_join=1 then 1 else 0 end) as nM1824,
 sum(case when dr.gender_code_iso=1 and AGE_IN_YEARS(dr.date_born) between 25 and 29 and r.is_join=1 then 1 else 0 end) as nM2529,
 sum(case when dr.gender_code_iso=1 and AGE_IN_YEARS(dr.date_born) between 30 and 39 and r.is_join=1 then 1 else 0 end) as nM3039,
 sum(case when dr.gender_code_iso=1 and AGE_IN_YEARS(dr.date_born) between 40 and 49 and r.is_join=1 then 1 else 0 end) as nM4049,
 sum(case when dr.gender_code_iso=1 and AGE_IN_YEARS(dr.date_born) between 50 and 64 and r.is_join=1 then 1 else 0 end) as nM5064,
 sum(case when dr.gender_code_iso=1 and AGE_IN_YEARS(dr.date_born)>64 and r.is_join=1 then 1 else 0 end) as nM65,
 sum(case when dr.gender_code_iso=2 and AGE_IN_YEARS(dr.date_born)<18 and r.is_join=1 then 1 else 0 end) as nF17,
 sum(case when dr.gender_code_iso=2 and AGE_IN_YEARS(dr.date_born) between 18 and 24 and r.is_join=1 then 1 else 0 end) as nF1824,
 sum(case when dr.gender_code_iso=2 and AGE_IN_YEARS(dr.date_born) between 25 and 29 then 1 else 0 end) as nF2529,
 sum(case when dr.gender_code_iso=2 and AGE_IN_YEARS(dr.date_born) between 30 and 39 then 1 else 0 end) as nF3039,
 sum(case when dr.gender_code_iso=2 and AGE_IN_YEARS(dr.date_born) between 40 and 49 then 1 else 0 end) as nF4049,
 sum(case when dr.gender_code_iso=2 and AGE_IN_YEARS(dr.date_born) between 50 and 64 then 1 else 0 end) as nF5064,
 sum(case when dr.gender_code_iso=2 and AGE_IN_YEARS(dr.date_born)>64 then 1 else 0 end) as nF65
FROM recruitment.respondent_cost r 
INNER JOIN wh.dim_respondent dr ON r.entity_id = dr.entity_id
WHERE r.month_id IS NOT null -->= (SELECT MAX(month_id) FROM  wh.fact_recruitment_cost )
GROUP BY
 ifnull(r.dim_respondent_source_id,0),
 r.sub_panel_id,
 r.dim_country_id,
 ifnull(r.dim_currency_id,1030),
 r.month_id
;



-----------------------------------------
--insert historical rows into temp table
-----------------------------------------
INSERT INTO fact_recruitment_cost_temp
 (
  dim_respondent_source_id,
  month_id,
  dim_country_id,
  sub_panel_id,
  dim_currency_id,
  total_recruit_cost,
  nPayment_Events,
  nLeads,
  nJoins,
  nM17,
  nM1824,
  nM2529,
  nM3039,
  nM4049,
  nM5064,
  nM65,
  nF17,
  nF1824,
  nF2529,
  nF3039,
  nF4049,
  nF5064,
  nF65,
  wh_datemod
 )  
SELECT
  dim_respondent_source_id,
  month_id,
  dim_country_id,
  sub_panel_id,
  dim_currency_id,
  total_recruit_cost,
  nPayment_Events,
  nLeads,
  nJoins,
  nM17,
  nM1824,
  nM2529,
  nM3039,
  nM4049,
  nM5064,
  nM65,
  nF17,
  nF1824,
  nF2529,
  nF3039,
  nF4049,
  nF5064,
  nF65,
  wh_datemod
FROM wh.fact_recruitment_cost
WHERE ( dim_respondent_source_id, month_id, dim_country_id, sub_panel_id )
 NOT IN ( SELECT dim_respondent_source_id, month_id, dim_country_id, sub_panel_id 
          FROM fact_recruitment_cost_temp )
;



--truncate the target table
TRUNCATE TABLE wh.fact_recruitment_cost;

--insert rows from temp table into target
INSERT INTO wh.fact_recruitment_cost
 (
  dim_respondent_source_id,
  month_id,
  dim_country_id,
  sub_panel_id,
  dim_currency_id,
  total_recruit_cost,
  nPayment_Events,
  nLeads,
  nJoins,
  nM17,
  nM1824,
  nM2529,
  nM3039,
  nM4049,
  nM5064,
  nM65,
  nF17,
  nF1824,
  nF2529,
  nF3039,
  nF4049,
  nF5064,
  nF65,
  wh_datemod
 )  
SELECT
 dim_respondent_source_id,
 month_id,
 dim_country_id,
 sub_panel_id,
 dim_currency_id,
 total_recruit_cost,
 nPayment_Events,
 nLeads,
 nJoins,
 nM17,
 nM1824,
 nM2529,
 nM3039,
 nM4049,
 nM5064,
 nM65,
 nF17,
 nF1824,
 nF2529,
 nF3039,
 nF4049,
 nF5064,
 nF65,
 wh_datemod
FROM fact_recruitment_cost_temp
;

--log event end
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'fact_recruitment_cost', 'end', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'fact_recruitment_cost', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.fact_recruitment_cost;