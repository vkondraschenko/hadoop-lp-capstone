
package com.griddynamics.training.spark.jobs

import java.sql.Timestamp

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{SaveMode, SparkSession}

object ProductTableLoader {

  case class Event(name: String, price: Double, purchaseDateTime: Timestamp, categoryName: String, clientIP: String)

  def executeQuery(query: String, targetTableName: String, init: SparkSession => Unit = { _ => () }): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Frequently Purchased Categories")
      .getOrCreate()

    import spark.implicits._

    try {
      val eventsDS = spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .load("hdfs://sandbox-hdp.hortonworks.com:8020/events/*/*/*/")
        .toDF("name", "price", "purchaseDateTime", "categoryName", "clientIP")
        .withColumn("price", col("price").cast(DoubleType))
        .as[Event]

      eventsDS.createOrReplaceTempView("products")

      init(spark)

      spark
        .sql(query)
        .write.format("jdbc")
        .option("url", "jdbc:mysql://localhost/catalog")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", targetTableName)
        .option("user", "root")
        .option("password", "hortonworks1")
        .mode(SaveMode.Overwrite)
        .save()

    } finally {
      spark.stop()
    }
  }
}
