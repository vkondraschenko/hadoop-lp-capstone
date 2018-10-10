
package com.griddynamics.training.spark.jobs

import java.io.{BufferedReader, IOException, InputStreamReader}

import scala.collection.JavaConverters._

@SerialVersionUID(100L)
class IpToGeoIdUDF(private val subnetMasks: List[IpSubnetMask]) extends Serializable {
  def evaluate(ipAddress: String): Int = {
    val ipAddressToCheck = IpSubnetMask.ipAddressToInt(ipAddress)
    subnetMasks.find(_.matches(ipAddressToCheck)).map(_.getGeoNameId).getOrElse(-1)
  }
}

@SerialVersionUID(101L)
object IpToGeoIdUDF extends Serializable {

  def apply(): IpToGeoIdUDF = {
    val reader = new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream("GeoLite2-Country-Blocks-IPv4.csv")))
    try {
      val list = reader.lines.skip(1).iterator().asScala
        .map(s => s.split(","))
        .filter(v => v.length > 2 && v(0).length > 0 && v(1).length > 0)
        .map(v => IpSubnetMask(v(0), v(1).toInt))
        .toList
      list.take(10).foreach(println)
      new IpToGeoIdUDF(list)
    } catch {
      case e: IOException =>
        throw new RuntimeException(e)
    } finally {
      if (reader != null)
        reader.close()
    }
  }
}
