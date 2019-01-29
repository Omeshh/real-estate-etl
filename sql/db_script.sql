-- create role etl_user
-- create role bi_user

CREATE DATABASE real_estate;
create schema src;
create schema etl;
create schema dw;

CREATE INDEX ON etl.stage_real_estate((body->>'ok'));

-- Drop table

-- DROP TABLE src.stage_real_estate

CREATE TABLE src.stage_real_estate (
	id serial NOT NULL,
	file_datetime timestamp NULL,
	body jsonb NULL,
	CONSTRAINT stage_real_estate_pkey PRIMARY KEY (id)
);
CREATE INDEX ix_stage_real_estate_file_datetime ON src.stage_real_estate USING btree (file_datetime);


 DROP TABLE src.stage_real_estate;
drop table if exists etl.stage_dim_geography;
CREATE TABLE etl.stage_dim_geography (
	id serial NOT NULL,
	city_name varchar(50) null,
	country_code varchar(50) null,
	country_name varchar(50) null,
	postal_code	varchar(50) null,
	CONSTRAINT pk_stage_dim_geography PRIMARY KEY (id)
);



drop table if exists etl.stage_dim_agency;
CREATE TABLE etl.stage_dim_agency (
	id serial NOT NULL,
	customer_id varchar(50)  NULL,
	contact_details_id int NULL,
	city_name varchar(50) null,
	country_code varchar(50) null,
	country_name varchar(50) null,
	postal_code	varchar(50) null,	
    address_city varchar(50) NULL,
    address_houseNumber varchar(25) NULL,
    address_postcode varchar(10) NULL,
    address_street varchar(255) NULL,
    cell_phone_number varchar(20) NULL,
	company varchar(255) NULL,
	email varchar(255)  NULL,
	firstname varchar(255) NULL,
	lastname varchar(255)  NULL,
	salutation varchar(25) NULL,
	CONSTRAINT pk_stage_dim_agency PRIMARY KEY (id)
);



drop table if exists etl.stage_fact_flat;
CREATE TABLE etl.stage_fact_flat (
	id serial NOT NULL,
	real_estate_id int null,
	email varchar(255)  NULL,
	city_name varchar(50) null,
	country_code varchar(50) null,
	country_name varchar(50) null,
	postal_code	varchar(50) null,		
	house_number varchar(25),
	street varchar(25) null,
	latitude varchar(25) null,
	longitude varchar(25) null,	
	real_estate_type varchar(25)  null,
	apartment_type varchar(25) null,
	flat_size decimal(12,8) null,
	total_price money null,
	total_rent_scope varchar(10) null,	
	heating_costs money null,
	service_charge money null,
	total_rooms smallint null,
	total_sleeping_rooms smallint null,
	total_living_rooms smallint null,
	total_bath_rooms smallint null,	
	total_toilets smallint null,	
	status varchar(10) null,
	created_Date timestamptz null,
	updated_date timestamptz null,
	has_flats_pictures char(1) null,
	CONSTRAINT pk_stage_fact_flat PRIMARY KEY (id)
);











drop table if exists dw.dim_geography;
CREATE TABLE dw.dim_geography (
	geography_key serial NOT NULL,
	city_name varchar(50) null,
	country_code varchar(50) null,
	country_name varchar(50) null,
	postal_code	varchar(50) null,
	CONSTRAINT pk_dim_geography PRIMARY KEY (geography_key)
);
CREATE unique INDEX idx_dim_geography_city_postal ON dw.dim_geography USING btree (country_name, city_name, postal_code);



drop table if exists dw.dim_agency;
CREATE TABLE dw.dim_agency (
	agency_key serial NOT NULL,
	geography_key INT null,
	customer_id varchar(50)  NULL,
	contact_details_id int NULL,
    address_city varchar(50) NULL,
    address_houseNumber varchar(25) NULL,
    address_postcode varchar(10) NULL,
    address_street varchar(255) NULL,
	country_code varchar(5) null,
    cell_phone_number varchar(20) NULL,
	company varchar(255) NULL,
	email varchar(255)  NULL,
	firstname varchar(255) NULL,
	lastname varchar(255)  NULL,
	salutation varchar(25) NULL,
	CONSTRAINT pk_dim_agency PRIMARY KEY (agency_key)
);
CREATE unique INDEX idx_dim_agency_email ON dw.dim_agency USING btree (email);




drop table if exists dw.fact_flat;
CREATE TABLE dw.fact_flat (
	flat_key serial NOT NULL,
	real_estate_id int null,
	geography_key INT null,	
	agency_key int  null,
	address_key int  null,
	house_number varchar(25),
	street varchar(25) null,
	latitude varchar(25) null,
	longitude varchar(25) null,	
	real_estate_type varchar(25)  null,
	apartment_type varchar(25) null,
	flat_size decimal(12,8) null,
	total_price money null,
	total_rent_scope varchar(10) null,	
	heating_costs money null,
	service_charge money null,
	total_rooms smallint null,
	total_sleeping_rooms smallint null,
	total_living_rooms smallint null,
	total_bath_rooms smallint null,	
	total_toilets smallint null,	
	status varchar(10) null,
	created_Date timestamptz null,
	updated_date timestamptz null,
	has_flats_pictures char(1) null,
	CONSTRAINT pk_fact_flat PRIMARY KEY (flat_key)
);
CREATE unique INDEX idx_fact_flat_agency_key ON dw.fact_flat USING btree (real_estate_id, geography_key, agency_key);



	
	
	
	
	EffectiveDateKey int not null,
	ExpirationDateKey int null,
	IsCurrentRecord bool not null default 1,
	IsRowDeleted bool not null default 'N',
	
	
	