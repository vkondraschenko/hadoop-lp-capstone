CREATE DATABASE IF NOT EXISTS catalog;

CREATE EXTERNAL TABLE catalog.products(name string, price decimal(6,2), purchaseDateTime timestamp, categoryName string, clientIP string)
PARTITIONED BY (purchaseDate string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/events';

ALTER TABLE catalog.products ADD PARTITION (purchaseDate='2018-09-01') LOCATION '/events/2018/09/01';
ALTER TABLE catalog.products ADD PARTITION (purchaseDate='2018-09-02') LOCATION '/events/2018/09/02';
ALTER TABLE catalog.products ADD PARTITION (purchaseDate='2018-09-03') LOCATION '/events/2018/09/03';
ALTER TABLE catalog.products ADD PARTITION (purchaseDate='2018-09-04') LOCATION '/events/2018/09/04';
ALTER TABLE catalog.products ADD PARTITION (purchaseDate='2018-09-05') LOCATION '/events/2018/09/05';
ALTER TABLE catalog.products ADD PARTITION (purchaseDate='2018-09-06') LOCATION '/events/2018/09/06';
ALTER TABLE catalog.products ADD PARTITION (purchaseDate='2018-09-07') LOCATION '/events/2018/09/07';

CREATE TABLE catalog.country_locations_staging (geoname_id int,locale_code string,continent_code string,continent_name string,
country_iso_code string,country_name string,is_in_european_union boolean)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/admin/geolite2/GeoLite2-Country-Locations';

CREATE TABLE catalog.country_locations
STORED AS ORC
AS
SELECT geoname_id, country_name
FROM catalog.country_locations_staging;







