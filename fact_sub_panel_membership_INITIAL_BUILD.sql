/*   Modified By Raj to Switch to V2*/


DROP TABLE IF EXISTS bi.fact_sub_panel_membership_V2 CASCADE;


DROP TABLE IF EXISTS bi_stage.fact_sub_panel_membership_step01_V2 ;
CREATE TABLE bi_stage.fact_sub_panel_membership_step01_V2(
entity_id           INT NOT NULL,
sub_panel_id        INT NOT NULL,
membership_status_enum_value_id INT NOT NULL,
modified_at         TIMESTAMP NOT NULL,
wh_datemod          TIMESTAMP DEFAULT SYSDATE  ,
sort_key            INT);



DROP TABLE IF EXISTS bi_stage.fact_sub_panel_membership_step02_V2;

CREATE TABLE bi_stage.fact_sub_panel_membership_step02_V2(
entity_id           INT,
sub_panel_id        INT,
membership_status_enum_value_id INT,
dim_respondent_key  INT NOT NULL,
dim_sub_panel_key   INT NOT NULL,
dim_membership_status_key INT,
start_date          TIMESTAMP NOT NULL,
end_date            TIMESTAMP,
is_current          BOOLEAN,
is_membership_status_active BOOLEAN,
wh_datemod          TIMESTAMP DEFAULT SYSDATE ,
fact_sub_panel_membership_key INT,
sort_key            INT);





DROP TABLE IF EXISTS bi_stage.fact_sub_panel_membership_step03_V2;


CREATE TABLE bi_stage.fact_sub_panel_membership_step03_V2(
entity_id           INT,
sub_panel_id        INT,
membership_status_enum_value_id INT,
dim_respondent_key  INT NOT NULL,
dim_sub_panel_key   INT NOT NULL,
dim_membership_status_key INT,
start_date          TIMESTAMP NOT NULL,
end_date            TIMESTAMP,
is_current          BOOLEAN,
is_membership_status_active BOOLEAN,
wh_datemod          TIMESTAMP DEFAULT SYSDATE ,
fact_sub_panel_membership_key INT,
sort_key            INT);




CREATE TABLE bi.fact_sub_panel_membership_V2
(
    fact_sub_panel_membership_key  IDENTITY  ( 10000 ) ,
    dim_respondent_key int NOT NULL,
    dim_sub_panel_key int NOT NULL,
    dim_membership_status_key int NOT NULL,
    start_date timestamp NOT NULL,
    end_date timestamp,
    is_current boolean NOT NULL,
    is_membership_status_active boolean NOT NULL,
    wh_datemod timestamp DEFAULT "sysdate"()
)
PARTITION BY (date_part('year', fact_sub_panel_membership_V2.start_date));

ALTER TABLE bi.fact_sub_panel_membership_V2 ADD CONSTRAINT fact_sub_panel_membership_pk PRIMARY KEY (fact_sub_panel_membership_key); 

ALTER TABLE bi.fact_sub_panel_membership_V2 ADD CONSTRAINT dim_respondent_fact_sub_panel_membership_FK1 FOREIGN KEY (dim_respondent_key) references bi.dim_respondent (dim_respondent_key);
ALTER TABLE bi.fact_sub_panel_membership_V2 ADD CONSTRAINT dim_membership_status_fact_sub_panel_membership_FK1 FOREIGN KEY (dim_membership_status_key) references bi.dim_membership_status (dim_membership_status_key);
ALTER TABLE bi.fact_sub_panel_membership_V2 ADD CONSTRAINT dim_sub_panel_fact_sub_panel_membership_FK1 FOREIGN KEY (dim_sub_panel_key) references bi.dim_sub_panel (dim_sub_panel_key);

CREATE PROJECTION bi.fact_sub_panel_membership_V2_super
(
 fact_sub_panel_membership_key,
 dim_respondent_key,
 dim_sub_panel_key,
 dim_membership_status_key,
 start_date,
 end_date,
 is_current,
 is_membership_status_active,
 wh_datemod
)
AS
 SELECT fact_sub_panel_membership_V2.fact_sub_panel_membership_key,
        fact_sub_panel_membership_V2.dim_respondent_key,
        fact_sub_panel_membership_V2.dim_sub_panel_key,
        fact_sub_panel_membership_V2.dim_membership_status_key,
        fact_sub_panel_membership_V2.start_date,
        fact_sub_panel_membership_V2.end_date,
        fact_sub_panel_membership_V2.is_current,
        fact_sub_panel_membership_V2.is_membership_status_active,
        fact_sub_panel_membership_V2.wh_datemod
 FROM bi.fact_sub_panel_membership_V2
 ORDER BY fact_sub_panel_membership_V2.fact_sub_panel_membership_key
SEGMENTED BY hash(fact_sub_panel_membership_V2.fact_sub_panel_membership_key) ALL NODES ;

CREATE PROJECTION bi.fact_sub_panel_membership_V2_buddy
(
 fact_sub_panel_membership_key,
 dim_respondent_key,
 dim_sub_panel_key,
 dim_membership_status_key,
 start_date,
 end_date,
 is_current,
 is_membership_status_active,
 wh_datemod
)
AS
 SELECT fact_sub_panel_membership_V2.fact_sub_panel_membership_key,
        fact_sub_panel_membership_V2.dim_respondent_key,
        fact_sub_panel_membership_V2.dim_sub_panel_key,
        fact_sub_panel_membership_V2.dim_membership_status_key,
        fact_sub_panel_membership_V2.start_date,
        fact_sub_panel_membership_V2.end_date,
        fact_sub_panel_membership_V2.is_current,
        fact_sub_panel_membership_V2.is_membership_status_active,
        fact_sub_panel_membership_V2.wh_datemod
 FROM bi.fact_sub_panel_membership_V2
 ORDER BY fact_sub_panel_membership_V2.is_current,
          fact_sub_panel_membership_V2.dim_membership_status_key,
          fact_sub_panel_membership_V2.dim_sub_panel_key,
          fact_sub_panel_membership_V2.start_date,
          fact_sub_panel_membership_V2.fact_sub_panel_membership_key
SEGMENTED BY hash(fact_sub_panel_membership_V2.fact_sub_panel_membership_key) ALL NODES OFFSET 1;

CREATE PROJECTION bi.fact_sub_panel_membership_V2_buddy02
(
 fact_sub_panel_membership_key,
 dim_respondent_key,
 dim_sub_panel_key,
 dim_membership_status_key,
 start_date,
 end_date,
 is_current,
 is_membership_status_active,
 wh_datemod
)
AS
 SELECT fact_sub_panel_membership_V2.fact_sub_panel_membership_key,
        fact_sub_panel_membership_V2.dim_respondent_key,
        fact_sub_panel_membership_V2.dim_sub_panel_key,
        fact_sub_panel_membership_V2.dim_membership_status_key,
        fact_sub_panel_membership_V2.start_date,
        fact_sub_panel_membership_V2.end_date,
        fact_sub_panel_membership_V2.is_current,
        fact_sub_panel_membership_V2.is_membership_status_active,
        fact_sub_panel_membership_V2.wh_datemod
 FROM bi.fact_sub_panel_membership_V2
 ORDER BY fact_sub_panel_membership_V2.dim_respondent_key
SEGMENTED BY hash(fact_sub_panel_membership_V2.fact_sub_panel_membership_key) ALL NODES OFFSET 1;







--truncate the staging table
--TRUNCATE TABLE bi_stage.fact_sub_panel_membership_step01_V2;

--data from fvalues1_history
INSERT INTO bi_stage.fact_sub_panel_membership_step01_V2
( 
	  entity_id
	, sub_panel_id
	, membership_status_enum_value_id
	, modified_at
	)
SELECT 	f1.entity_id
	, spf.sub_panel_id
	, f1.value AS membership_status_enum_value_id
	, f1.modified_at
 FROM panel.fvalues1_history f1
INNER JOIN panel.Sub_panel_features spf
	ON ( 
		  spf.feature_id = f1.feature_id AND
 	   	spf.feature_order = 106 --Subpanel Membership Status
 	   	)
;

COMMIT;

--data from fvalues1
INSERT INTO bi_stage.fact_sub_panel_membership_step01_V2
( 
	  entity_id
	, sub_panel_id
	, membership_status_enum_value_id
	, modified_at
	)
SELECT 
	f1.entity_id
	, spf.sub_panel_id
	, f1.value AS membership_status_enum_value_id
	, f1.modified_at
FROM panel.fvalues1 f1
INNER JOIN panel.Sub_panel_features spf
	ON ( 
		  spf.feature_id = f1.feature_id AND
 	   	spf.feature_order = 106 --Subpanel Membership Status
 	   	)
;

COMMIT;

--dimension key lookup
 

INSERT INTO bi_stage.fact_sub_panel_membership_step02_V2
  	(
  		  entity_id
  		, sub_panel_id
  		, membership_status_enum_value_id
  		, dim_respondent_key
  		, dim_sub_panel_key
  		, dim_membership_status_key
  		, start_date
  		, is_membership_status_active
  		)
  SELECT 
	  s1.entity_id
	, s1.sub_panel_id
	, s1.membership_status_enum_value_id
	, COALESCE( r.dim_respondent_key, 0 ) AS dim_respondent_key
	, COALESCE( sp.dim_sub_panel_key, 0 ) AS dim_sub_panel_key
	, COALESCE( ms.dim_membership_status_key, 0 ) AS dim_membership_status_key
	, modified_at AS start_date
	, COALESCE( ms.is_active, FALSE ) AS is_membership_status_active
FROM bi_stage.fact_sub_panel_membership_step01_V2 s1
LEFT OUTER JOIN bi.dim_respondent r ON s1.entity_id = r.entity_id
LEFT OUTER JOIN bi.dim_sub_panel sp ON s1.sub_panel_id = sp.sub_panel_id
LEFT OUTER JOIN bi.dim_membership_status ms ON s1.membership_status_enum_value_id = ms.membership_status_enum_value_id
;

COMMIT;

--dedupe 
 

INSERT INTO bi_stage.fact_sub_panel_membership_step03_V2
 	 (
	  entity_id
	, sub_panel_id
	, membership_status_enum_value_id
	, fact_sub_panel_membership_key
	, dim_respondent_key
	, dim_sub_panel_key
	, dim_membership_status_key
	, start_date
	, end_date
	, is_current
	, is_membership_status_active
	)
SELECT
	  entity_id
	, sub_panel_id
	, membership_status_enum_value_id
	, fact_sub_panel_membership_key
	, dim_respondent_key
	, dim_sub_panel_key
	, dim_membership_status_key
	, start_date
	, end_date
	, CASE WHEN end_date IS NULL THEN TRUE ELSE FALSE END AS is_current
	, is_membership_status_active
FROM 
(
SELECT 
	  x.entity_id
	, x.sub_panel_id
	, x.membership_status_enum_value_id
	, x.fact_sub_panel_membership_key
	, x.dim_respondent_key
	, x.dim_sub_panel_key
	, x.dim_membership_status_key
	, x.start_date
	, LEAD( x.start_date, 1 ) OVER ( PARTITION BY x.dim_respondent_key, x.dim_sub_panel_key ORDER BY x.start_date ) - INTERVAL '1 MICROSECOND'  AS end_date
	, x.is_membership_status_active
FROM 
(
SELECT 
      entity_id
	, sub_panel_id
	, membership_status_enum_value_id
	, fact_sub_panel_membership_key
	, dim_respondent_key
	, dim_sub_panel_key
	, dim_membership_status_key
	, LAG(dim_membership_status_key, 1 ) OVER (PARTITION BY dim_respondent_key, dim_sub_panel_key ORDER BY start_date ) AS previous_dim_membership_status_key
	, start_date
	, is_membership_status_active	
FROM bi_stage.fact_sub_panel_membership_step02_V2
) x
WHERE 
	x.previous_dim_membership_status_key IS NULL 
	OR NOT ( x.dim_membership_status_key = x.previous_dim_membership_status_key )
) y
;
COMMIT;

--PUSH ROWS TO FACT TABLE

INSERT INTO bi.fact_sub_panel_membership_V2
  	(
		  dim_respondent_key
		, dim_sub_panel_key
		, dim_membership_status_key
		, start_date
		, end_date
		, is_current
		, is_membership_status_active
		)
SELECT
	  dim_respondent_key
		, dim_sub_panel_key
		, dim_membership_status_key
		, start_date
		, end_date
		, is_current
		, is_membership_status_active
FROM bi_stage.fact_sub_panel_membership_step03_V2 
WHERE fact_sub_panel_membership_key IS NULL
AND dim_respondent_key > 0
;

COMMIT;

--cleanup
TRUNCATE TABLE bi_stage.fact_sub_panel_membership_step01_V2;
TRUNCATE TABLE bi_stage.fact_sub_panel_membership_step02_V2;
--TRUNCATE TABLE bi_stage.fact_sub_panel_membership_step03; --must be retained for next round of processing