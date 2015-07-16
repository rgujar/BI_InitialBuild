--log event start
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_respondent_cost', 'start', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_respondent_cost', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM recruitment.respondent_cost;

TRUNCATE TABLE recruitment.respondent_cost;

/* 
-----------------------------------------------
                ----- EPANEL ------
-----------------------------------------------
*/
INSERT INTO recruitment.respondent_cost
(
  sourcesystemid
, source_panelist_id
, sub_panel_id 
, entity_id
, cost
, dim_currency_id
, dim_respondent_source_id
, external_system_member_id 
, dim_country_id
, date_membership_started
, month_id
, is_join
) 
SELECT 
   1 AS sourcesystemid --Epanel
   , e.entity_id as source_panelist_id
   , e.sub_panel_id
   , e.entity_id 
   , e.price AS cost
   , crncy.dim_currency_id
   , rs.dim_respondent_source_id
   , CAST( NULL AS VARCHAR(400) ) AS external_system_member_id
   , e.dim_country_id
   , e.date_membership_started
   , m.month_id
   , 1 AS is_join
    FROM recruitment.epanel_legacy_recruitment_cost e
    INNER JOIN WH.dim_currency crncy ON e.price_currency_iso = crncy.currency_iso_code
    INNER JOIN WH.dim_respondent_source rs ON e.epanel_promotionunitid = rs.epanel_promotionunitid
    INNER JOIN WH.dim_month m ON e.date_membership_started BETWEEN m.month_start_date AND m.month_end_date
   WHERE e.price IS NOT NULL
 ;



/* 
-----------------------------------------------
                ----- GOLDRUSH ------
-----------------------------------------------
*/
INSERT INTO recruitment.respondent_cost
(
  sourcesystemid
, source_panelist_id
, sub_panel_id 
, entity_id
, cost
, dim_currency_id
, dim_respondent_source_id
, external_system_member_id 
, dim_country_id
, date_membership_started
, month_id
, is_join
) 
SELECT 
    11 AS sourcesystemid --Goldrush
   , o.panelid as source_panelist_id
   , o.sub_panel_id
   , o.entity_id
   , COALESCE(o.cost, 0) AS cost
   , crncy.dim_currency_id
   , rs.dim_respondent_source_id
   , CAST( o.panelid AS VARCHAR(400) )  AS external_system_member_id
   , COALESCE(o.dim_country_id, 227) AS dim_country_id
   , o.date_membership_started
   , m.month_id
   , 1 AS join
FROM recruitment.oo_legacy_recruitment_detail o
INNER JOIN WH.dim_currency crncy ON COALESCE(o.cost_currency_iso, 'USD') = crncy.currency_iso_code
INNER JOIN WH.dim_respondent_source rs  ON o.affiliate_campaign_id = rs.goldrush_affiliatecampaignid
INNER JOIN WH.dim_month m ON o.date_membership_started BETWEEN m.month_start_date AND m.month_end_date
;
 


/* 
----------------------------------------------------------
   ----- RMT, HasOffers Web, HasOffers Mobile ------
----------------------------------------------------------
*/

DROP TABLE IF EXISTS respondent_cost_temp;

CREATE LOCAL TEMPORARY TABLE respondent_cost_temp
ON COMMIT PRESERVE ROWS
AS
SELECT 
   rs.sourcesystemid
  , spm.entity_id as source_panelist_id
  , spm.sub_panel_id
  , spm.entity_id
  , CASE 
     WHEN rs.sourcesystemid = 6 THEN --RMT
       CASE
           WHEN cmpn.typeId = 1 THEN cmpn.price
           WHEN cmpn.typeId = 6 THEN cmpn.price
           WHEN spm.membershipstatusid > 3 THEN cmpn.price
        ELSE 0
        END 
     WHEN rs.sourcesystemid = 12 THEN hrc.cost --HasOffers Web
     WHEN rs.sourcesystemid = 14 THEN hrc.cost --HasOffers Mobile
   ELSE NULL 
   END AS cost
  , CASE 
      WHEN rs.sourcesystemid = 6 THEN rmt_crncy.dim_currency_id  --RMT
      WHEN rs.sourcesystemid = 12 THEN hrc.dim_currency_id --HasOffers Web
      WHEN rs.sourcesystemid = 14 THEN hrc.dim_currency_id --HasOffers Mobile
     ELSE NULL
     END AS dim_currency_id
  , spm.dim_respondent_source_id
  , spm.external_system_member_id
  , ctry.dim_country_id
  , spm.date_membership_started
  , spm.date_membership_status_mod
  , TO_NUMBER( TO_CHAR(  COALESCE(spm.date_membership_started, spm.date_membership_status_mod), 'YYYYMM' ) ) AS membership_status_yearmo
  , cast(Null as INT ) as month_id
  FROM wh.dim_subpanel_membership spm
  INNER JOIN wh.dim_sub_panel sp ON spm.sub_panel_id = sp.sub_panel_id
  INNER JOIN wh.dim_channel chnl ON ( sp.dim_channel_id = chnl.dim_channel_id AND chnl.channel_name NOT IN ('Partner', 'UMAS') )
  INNER JOIN WH.dim_respondent_source rs ON spm.dim_respondent_source_id = rs.dim_respondent_source_id
  INNER JOIN (
				SELECT * 
				FROM panel.fvalues1 
				WHERE feature_id = 17
		   ) r ON spm.entity_id = r.entity_id
  INNER JOIN WH.dim_country ctry ON r.value = ctry.country_enum_value_id 
  LEFT OUTER JOIN recruitment.hasoffers_respondent_cost hrc ON ( spm.entity_id = hrc.entity_id AND spm.sub_panel_id = hrc.sub_panel_id ) 
  LEFT OUTER JOIN reporting.rmt_campaign cmpn ON rs.rmt_campaignid = cmpn.campaignId
  LEFT OUTER JOIN wh.dim_currency rmt_crncy ON cmpn.CurrencyID = rmt_crncy.rmt_currency_id
 WHERE rs.sourcesystemid IN ( 6, 12, 14 ) --RMT, HasOffers Web, HasOffers Mobile
 ORDER BY TO_NUMBER( TO_CHAR(  COALESCE(spm.date_membership_started, spm.date_membership_status_mod), 'YYYYMM' ) ), spm.sub_panel_id, spm.entity_id
 ;
 
 
 
 --populate month_id column
 UPDATE respondent_cost_temp
 SET month_id = m.month_id
 FROM wh.dim_month m
 WHERE respondent_cost_temp.membership_status_yearmo = m.yearmo
 ;
 
 
 
  
--insert into target
INSERT INTO recruitment.respondent_cost
(
  sourcesystemid
, source_panelist_id
, sub_panel_id 
, entity_id
, cost
, dim_currency_id
, dim_respondent_source_id
, external_system_member_id 
, dim_country_id
, date_membership_started
, month_id
, is_join
)
 SELECT
    b.sourcesystemid
  , b.source_panelist_id
  , b.sub_panel_id
  , b.entity_id
  , b.cost
  , b.dim_currency_id
  , b.dim_respondent_source_id
  , b.external_system_member_id
  , b.dim_country_id
  , COALESCE(b.date_membership_started, b.date_membership_status_mod) AS date_membership_started
  , b.month_id
  ,CASE
       WHEN b.membershipstatus_change_rank = 1 AND b.status_change_type_id IN(2,5) AND b.date_membership_started IS NOT NULL THEN 1
    ELSE 0
   END AS is_join
FROM
(
SELECT
    a.sourcesystemid
  , a.source_panelist_id
  , a.sub_panel_id
  , a.entity_id
  , a.cost
  , a.dim_currency_id
  , a.dim_respondent_source_id
  , a.external_system_member_id
  , a.dim_country_id
  , a.month_id
  , msh.status_change_type_id
  , a.date_membership_started
  , a.date_membership_status_mod
  , a.membership_status_yearmo
  , RANK() OVER (PARTITION BY a.entity_id, a.sub_panel_id, msh.status_change_type_id ORDER BY msh.change_date) AS membershipstatus_change_rank
FROM respondent_cost_temp a
 LEFT OUTER JOIN wh.membership_status_history msh ON ( a.entity_id = msh.entity_id AND a.sub_panel_id = msh.sub_panel_id AND msh.yearmo = a.membership_status_yearmo AND msh.status_change_type_id IN(2,5) )
 ) b
;

--log event end
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_respondent_cost', 'end', SYSDATE );   
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_respondent_cost', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM recruitment.respondent_cost;