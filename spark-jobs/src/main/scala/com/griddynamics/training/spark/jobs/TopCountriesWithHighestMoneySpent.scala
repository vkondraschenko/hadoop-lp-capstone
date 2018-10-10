
package com.griddynamics.training.spark.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes

object TopCountriesWithHighestMoneySpent {

  case class CountryLocation(geoId: Int, countryName: String)

  def initUDF(spark: SparkSession): Unit = {
    import spark.implicits._

    val countryLocationDS = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .option("treatEmptyValuesAsNulls", "true")
      .load("hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/geolite2/GeoLite2-Country-Locations/")
      .select(col("geoname_id").cast(DataTypes.IntegerType).as("geoId"), col("country_name").cast(DataTypes.StringType).as("countryName"))
      .as[CountryLocation]

    countryLocationDS.createOrReplaceTempView("country_locations")

    val ipToGeoIdUDF: IpToGeoIdUDF =  IpToGeoIdUDF.apply()
    spark.udf.register("ip_to_geoid", ipToGeoIdUDF.evaluate _)
  }

  def main(args: Array[String]) {
    ProductTableLoader.executeQuery(
      "select cl.countryName, round(sum(t.price), 2) as money_spent FROM " +
        "(select price, ip_to_geoid(clientIP) as geoid from products) t " +
        "join country_locations cl on t.geoid = cl.geoId group by cl.countryName order by money_spent desc limit 10",
      "spark_top_10_countries_with_highest_money_spending",
      initUDF
    )
  }
}
