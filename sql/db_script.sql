-- Create databases:

-- CREATE DATABASE real_estate;

-- CREATE ROLE etl_user;
-- CREATE ROLE bi_user;

CREATE SCHEMA src;
CREATE SCHEMA etl;
CREATE SCHEMA dwh;

-- DROP TABLE IF EXISTS src.real_estate_immobilienscout24;

-- Create stage tables:
-- DROP TABLE IF EXISTS etl.stage_dim_geography;
CREATE TABLE etl.stage_dim_geography (
	id serial NOT NULL,
	city_name varchar(50) NULL,
	country_code varchar(50) NULL,
	country_name varchar(50) NULL,
	postal_code varchar(50) NULL,
	CONSTRAINT pk_stage_dim_geography PRIMARY KEY (id)
);


-- DROP TABLE IF EXISTS etl.stage_dim_agency;
CREATE TABLE etl.stage_dim_agency (
	id serial NOT NULL,
	contact_details_id int NULL,
	customer_id varchar(50) NULL,
	email varchar(255) NULL,
	city_name varchar(50) NULL,
	country_code varchar(50) NULL,
	country_name varchar(50) NULL,
	postal_code varchar(50) NULL,
	house_number varchar(25) NULL,
	street varchar(255) NULL,
	cell_phone_number varchar(50) NULL,
	company varchar(255) NULL,
	first_name varchar(255) NULL,
	last_name varchar(255) NULL,
	salutation varchar(25) NULL,
	CONSTRAINT pk_stage_dim_agency PRIMARY KEY (id)
);


-- DROP TABLE IF EXISTS etl.stage_fact_flat;
CREATE TABLE etl.stage_fact_flat (
	id serial NOT NULL,
	file_date_key int NOT NULL,
	file_time_key char(8) NULL,
	real_estate_id int NULL,
	contact_details_id int NULL,
	city_name varchar(50) NULL,
	country_code varchar(50) NULL,
	country_name varchar(50) NULL,
	postal_code varchar(50) NULL,
	house_number varchar(25) NULL,
	street varchar(255) NULL,
	latitude varchar(25) NULL,
	longitude varchar(25) NULL,
	real_estate_type varchar(25) NULL,
	apartment_type varchar(25) NULL,
	flat_size numeric(12,8) NULL,
	total_price money NULL,
	total_rent_scope varchar(50) NULL,
	additional_costs money NULL,
	total_rooms float8 NULL,
	total_bathrooms int2 NULL,
	total_bedrooms int2 NULL,
	total_parking_spaces int2 NULL,
	has_attachments varchar(10) NULL,
	status varchar(10) NULL,
	created_date_key int NULL,
	created_time_key char(8) NULL,
	modified_date_key int NULL,
	modified_time_key char(8) NULL,
	CONSTRAINT pk_stage_fact_flat PRIMARY KEY (id)
);


-- Create dimension and fact tables:
-- DROP TABLE IF EXISTS dwh.dim_date;
CREATE TABLE dwh.dim_date (
	date_key int NOT NULL,
	date_actual date NOT NULL,
	epoch int8 NOT NULL,
	day_suffix varchar(4) NOT NULL,
	day_name varchar(9) NOT NULL,
	day_of_week int NOT NULL,
	day_of_month int NOT NULL,
	day_of_quarter int NOT NULL,
	day_of_year int NOT NULL,
	week_of_month int NOT NULL,
	week_of_year int NOT NULL,
	week_of_year_iso char(10) NOT NULL,
	month_actual int NOT NULL,
	month_name varchar(9) NOT NULL,
	month_name_abbreviated char(3) NOT NULL,
	quarter_actual int NOT NULL,
	quarter_name varchar(9) NOT NULL,
	year_actual int NOT NULL,
	first_day_of_week date NOT NULL,
	last_day_of_week date NOT NULL,
	first_day_of_month date NOT NULL,
	last_day_of_month date NOT NULL,
	first_day_of_quarter date NOT NULL,
	last_day_of_quarter date NOT NULL,
	first_day_of_year date NOT NULL,
	last_day_of_year date NOT NULL,
	mmyyyy char(6) NOT NULL,
	mmddyyyy char(10) NOT NULL,
	weekend_indr bool NOT NULL,
	CONSTRAINT dim_date_date_key_pk PRIMARY KEY (date_key)
);
CREATE INDEX dim_date_date_actual_idx ON dwh.dim_date USING btree (date_actual);


-- DROP TABLE IF EXISTS dwh.dim_time;
CREATE TABLE dwh.dim_time (
	time_key char(8) NOT NULL,
	full_time time NOT NULL,
	hour_num int2 NOT NULL,
	minute_num int2 NOT NULL,
	second_num int2 NOT NULL,
	minute_of_day_num int2 NOT NULL,
	time_12_hour char(8) NOT NULL,
	meridiem_indicator char(2) NOT NULL,
	CONSTRAINT dim_time_time_key_pk PRIMARY KEY (time_key)
);
CREATE INDEX dim_time_full_time_uindex ON dwh.dim_time USING btree (full_time);
     

-- DROP TABLE IF EXISTS dwh.dim_geography;
CREATE TABLE dwh.dim_geography (
	geography_key serial NOT NULL,
	city_name varchar(50) NULL,
	country_code varchar(50) NULL,
	country_name varchar(50) NULL,
	postal_code varchar(50) NULL,
	CONSTRAINT pk_dim_geography PRIMARY KEY (geography_key)
);
CREATE UNIQUE INDEX idx_dim_geography_city_postal ON dwh.dim_geography USING btree (city_name, country_name, country_code, postal_code);


-- DROP TABLE IF EXISTS dwh.dim_agency;
CREATE TABLE dwh.dim_agency (
	agency_key serial NOT NULL,
	contact_details_id int NULL,
	geography_key int NULL,
	customer_id varchar(50) NULL,
	email varchar(255) NULL,
	house_number varchar(25) NULL,
	street varchar(255) NULL,
	cell_phone_number varchar(50) NULL,
	company varchar(255) NULL,
	first_name varchar(255) NULL,
	last_name varchar(255) NULL,
	salutation varchar(25) NULL,
	CONSTRAINT pk_dim_agency PRIMARY KEY (agency_key)
);
CREATE UNIQUE INDEX idx_dim_agency_contact_details_id ON dwh.dim_agency USING btree (contact_details_id);


-- DROP TABLE IF EXISTS dwh.fact_flat;
CREATE TABLE dwh.fact_flat (
	flat_key serial NOT NULL,
	file_date_key int NOT NULL,
	file_time_key char(8) NULL,
	real_estate_id int NULL,
	agency_key int NULL,
	geography_key int NULL,
	house_number varchar(25) NULL,
	street varchar(255) NULL,
	latitude varchar(25) NULL,
	longitude varchar(25) NULL,
	real_estate_type varchar(25) NULL,
	apartment_type varchar(25) NULL,
	flat_size numeric(12,8) NULL,
	total_price money NULL,
	total_rent_scope varchar(50) NULL,
	additional_costs money NULL,
	total_rooms float8 NULL,
	total_bathrooms int2 NULL,
	total_bedrooms int2 NULL,
	total_parking_spaces int2 NULL,
	has_attachments varchar(10) NULL,
	status varchar(10) NULL,
	created_date_key int NULL,
	created_time_key char(8) NULL,
	modified_date_key int NULL,
	modified_time_key char(8) NULL,
	CONSTRAINT pk_fact_flat PRIMARY KEY (flat_key)
);
CREATE UNIQUE INDEX idx_fact_flat_agency_key ON dwh.fact_flat USING btree (real_estate_id, file_date_key, file_time_key);


-- Initialize dimension tables:
INSERT INTO dwh.dim_date
SELECT TO_CHAR(datum,'yyyymmdd')::INT AS date_dim_id,
       datum AS date_actual,
       EXTRACT(epoch FROM datum) AS epoch,
       TO_CHAR(datum,'fmDDth') AS day_suffix,
       TO_CHAR(datum,'Day') AS day_name,
       EXTRACT(isodow FROM datum) AS day_of_week,
       EXTRACT(DAY FROM datum) AS day_of_month,
       datum - DATE_TRUNC('quarter',datum)::DATE +1 AS day_of_quarter,
       EXTRACT(doy FROM datum) AS day_of_year,
       TO_CHAR(datum,'W')::INT AS week_of_month,
       EXTRACT(week FROM datum) AS week_of_year,
       TO_CHAR(datum,'YYYY"-W"IW-') || EXTRACT(isodow FROM datum) AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum,'Month') AS month_name,
       TO_CHAR(datum,'Mon') AS month_name_abbreviated,
       EXTRACT(quarter FROM datum) AS quarter_actual,
       CASE
         WHEN EXTRACT(quarter FROM datum) = 1 THEN 'First'
         WHEN EXTRACT(quarter FROM datum) = 2 THEN 'Second'
         WHEN EXTRACT(quarter FROM datum) = 3 THEN 'Third'
         WHEN EXTRACT(quarter FROM datum) = 4 THEN 'Fourth'
       END AS quarter_name,
       EXTRACT(isoyear FROM datum) AS year_actual,
       datum +(1 -EXTRACT(isodow FROM datum))::INT AS first_day_of_week,
       datum +(7 -EXTRACT(isodow FROM datum))::INT AS last_day_of_week,
       datum +(1 -EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH',datum) +INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter',datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter',datum) +INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(isoyear FROM datum) || '-01-01','YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(isoyear FROM datum) || '-12-31','YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum,'mmyyyy') AS mmyyyy,
       TO_CHAR(datum,'mmddyyyy') AS mmddyyyy,
       CASE
         WHEN EXTRACT(isodow FROM datum) IN (6,7) THEN TRUE
         ELSE FALSE
       END AS weekend_indr
FROM (SELECT '1970-01-01'::DATE+ SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES (0,29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;


INSERT INTO dwh.dim_time
SELECT	TO_CHAR(full_time,'hh24:mi:ss')
        , full_time :: TIME
     	, EXTRACT(HOUR FROM full_time)                                       AS hour_num
        , EXTRACT(MINUTE FROM full_time)                                     AS minute_num
        , EXTRACT(SECOND FROM full_time)                                     AS second_num
        -- Minute of the day (0 - 1439)
        , EXTRACT(HOUR FROM full_time) * 60 + EXTRACT(MINUTE FROM full_time) AS minute_of_day_num
        , TO_CHAR(full_time, 'hh12:mi am')                                   AS time_12_hour
        , TO_CHAR(full_time, 'am')                                           AS meridiem_indicator
FROM 	generate_series('2000-01-01 00:00:00' :: TIMESTAMP, '2000-01-01 23:59:59' :: TIMESTAMP,
						'1 SECOND' :: INTERVAL) AS t(full_time);
      