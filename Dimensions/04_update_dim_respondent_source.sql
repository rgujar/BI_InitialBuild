--Update Respondent Source Dimension

--log process start
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_dim_respondent_source', 'start', SYSDATE ); 
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_dim_respondent_source', 'start', SYSDATE, MAX(wh_datemod), '@'
FROM wh.dim_respondent_source;
/* 
********* DEPRECATED: NO LONGER NEEDED ***********
--Epanel
INSERT INTO wh.dim_respondent_source
 ( 
  respondent_source_name,
  sourcesystemid, 
  affiliate_id,
  affiliate_name,
  hasoffers_offerid_affiliateid, 
  rmt_campaignid,
  goldrush_affiliatecampaignid,
  epanel_promotionunitid
)
SELECT
 promotionname AS respondent_source_name,
 1 AS sourcesystemid,
 vendorid AS affiliate_id,
 vendorname AS affiliate_name ,
 CAST(NULL AS VARCHAR) AS hasoffers_offerid_affiliateid, 
 CAST( NULL AS INT) AS rmt_campaignid,
 CAST( NULL AS INT) AS goldrush_affiliatecampaignid,
 promotionunitid AS epanel_promotionunitid
FROM recruitment.epanel_vendor_lookup evl
LEFT OUTER JOIN wh.dim_respondent_source rs ON evl.promotionunitid = rs.epanel_promotionunitid
WHERE rs.epanel_promotionunitid IS NULL
;



*/

/* 
********* DEPRECATED: NO LONGER NEEDED ***********
--Goldrush / Opinion Outpost
INSERT INTO wh.dim_respondent_source
 ( 
  respondent_source_name,
  sourcesystemid, 
  affiliate_id,
  affiliate_name,
  hasoffers_offerid_affiliateid, 
  rmt_campaignid,
  goldrush_affiliatecampaignid,
  epanel_promotionunitid
)
SELECT
 cmpn.name AS respondent_source_name,
 11 AS sourcesystemid,
 cmpn.affiliateid AS affiliate_id,
 a.sitename AS affiliate_name ,
 CAST(NULL AS VARCHAR) AS hasoffers_offerid_affiliateid, 
 CAST( NULL AS INT) AS rmt_campaignid,
 affiliatecampaignid AS goldrush_affiliatecampaignid,
 CAST( NULL AS INT) AS epanel_promotionunitid
FROM recruitment.oo_legacy_campaign cmpn
LEFT OUTER JOIN recruitment.oo_legacy_affiliate a ON cmpn.affiliateid = a.affiliateid
LEFT OUTER JOIN wh.dim_respondent_source rs ON cmpn.affiliatecampaignid = rs.goldrush_affiliatecampaignid
WHERE rs.goldrush_affiliatecampaignid IS null
;


*/

--Recruit Management Tool (RMT)
INSERT INTO wh.dim_respondent_source
 ( 
  respondent_source_name,
  sourcesystemid, 
  affiliate_id,
  affiliate_name,
  hasoffers_offerid_affiliateid, 
  rmt_campaignid,
  goldrush_affiliatecampaignid,
  epanel_promotionunitid
)
SELECT
 cmpn.name AS respondent_source_name,
 6 AS sourcesystemid,
 cmpn.supplierId AS affiliate_id,
 s.cmp_name AS affiliate_name ,
 CAST(NULL AS VARCHAR) AS hasoffers_offerid_affiliateid, 
 cmpn.campaignId AS rmt_campaignid,
 CAST( NULL AS INT) AS goldrush_affiliatecampaignid,
 CAST( NULL AS INT) AS epanel_promotionunitid
FROM reporting.RMT_Campaign cmpn
LEFT OUTER JOIN reporting.cicmpy s ON cmpn.supplierId = s.cmp_code
LEFT OUTER JOIN wh.dim_respondent_source rs ON cmpn.campaignId = rs.rmt_campaignid
WHERE rs.rmt_campaignid IS NULL
;

--HasOffers Web Recruitment
INSERT INTO wh.dim_respondent_source
 ( 
   respondent_source_name,
   sourcesystemid, 
   affiliate_id,
   affiliate_name,
   hasoffers_offerid_affiliateid, 
  rmt_campaignid,
  goldrush_affiliatecampaignid,
  epanel_promotionunitid
)
SELECT 
 ho.offerid_affiliateid AS respondent_source_name,
 12 AS sourcesystemid,
 CAST( ho.affiliateid AS INT ) AS affiliate_id,
 TO_CHAR(ho.affiliateid) AS affiliate_name,
 ho.offerid_affiliateid AS hasoffers_offerid_affiliateid, 
 CAST( NULL AS INT) AS rmt_campaignid,
 CAST( NULL AS INT) AS goldrush_affiliatecampaignid,
 CAST( NULL AS INT) AS epanel_promotionunitid
FROM recruitment.v_hasoffers_offer ho
LEFT OUTER JOIN wh.dim_respondent_source rs ON ho.offerid_affiliateid = rs.hasoffers_offerid_affiliateid
WHERE rs.hasoffers_offerid_affiliateid IS NULL
;

--HasOffers Mobile Recruitment
INSERT INTO wh.dim_respondent_source
   ( 
    respondent_source_name
  , sourcesystemid
  )
SELECT 
	  a.respondent_source_name
	, a.sourcesystemid
FROM 
	(
	SELECT 
	 DISTINCT 
	   f4.value AS respondent_source_name
	 , 14 AS sourcesystemid
	FROM panel.fvalues4 f4
	INNER JOIN panel.Sub_panel_features spf ON spf.feature_id = f4.feature_id  AND  spf.feature_order = 2 --respondent source
	WHERE spf.sub_panel_id = 38	--QuickThoughts
	AND f4.yearmo BETWEEN TO_NUMBER( TO_CHAR( ADD_MONTHS( DATE_TRUNC('MONTH', SYSDATE), -1 ), 'YYYYMM' ) ) AND TO_NUMBER( TO_CHAR( DATE_TRUNC('MONTH', SYSDATE), 'YYYYMM' ) )
	) a
LEFT OUTER JOIN wh.dim_respondent_source rs ON a.respondent_source_name = rs.respondent_source_name AND a.sourcesystemid = rs.sourcesystemid
WHERE rs.respondent_source_name IS NULL 
; 

--log process end
--INSERT INTO work.event_log( process_name, event_type, event_date ) VALUES( 'update_dim_respondent_source', 'end', SYSDATE );   
INSERT INTO work.event_log( process_name, event_type, event_date, max_wh_datemod, run_id ) 
SELECT 'update_dim_respondent_source', 'end', SYSDATE, MAX(wh_datemod), '@'
FROM wh.dim_respondent_source;