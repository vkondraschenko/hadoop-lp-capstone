
package com.griddynamics.training.spark.jobs

import java.net.{InetAddress, UnknownHostException}

@SerialVersionUID(200L)
class IpSubnetMask(private val address: Int, private val mask: Int, val getGeoNameId: Int) extends Serializable {

  def matches(addressToCheck: Int): Boolean = (address & mask) == (addressToCheck & mask)

  override def toString = s"IpSubnetMask($address, $mask, $getGeoNameId)"
}

@SerialVersionUID(201L)
object IpSubnetMask extends Serializable {
  def apply(subnet: String, geoNameId: Int): IpSubnetMask = {
    val parts: Array[String] = subnet.split("/")
    val ip = parts(0)
    val prefix = if (parts.length < 2) 0 else parts(1).toInt

    new IpSubnetMask(ipAddressToInt(ip), -1 << (32 - prefix), geoNameId)
  }

  def ipAddressToInt(ipAddress: String): Int = {
    var a1: Option[InetAddress] = Option.empty
    try {
      a1 = Option(InetAddress.getByName(ipAddress))
    } catch {
      case _: UnknownHostException =>
    }
    a1.map(_.getAddress)
      .map(b1 => ((b1(0) & 0xFF) << 24) | ((b1(1) & 0xFF) << 16) | ((b1(2) & 0xFF) << 8) | ((b1(3) & 0xFF) << 0))
      .getOrElse(0)
  }
}