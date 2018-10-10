ADD JAR hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/geolite2/vk-custom-hive-udf-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION ip_to_geoid as 'com.griddynamics.training.vk.hive.udf.IpToGeoIdUDF';

CREATE TABLE catalog.top_10_countries_with_highest_money_spending
STORED AS ORC
AS
select cl.country_name, sum(t.price) as money_spent FROM
(select price, ip_to_geoid(clientIP) as geoid from catalog.products) t
join
catalog.country_locations cl on t.geoid = cl.geoname_id
group by cl.country_name
order by money_spent desc
limit 10;
