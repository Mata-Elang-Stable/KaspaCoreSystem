package me.mamotis.kaspacore.util

import cats.effect.IO
//import com.snowplowanalytics.maxmind.iplookups.IpLookups
import com.snowplowanalytics.maxmind.iplookups.CreateIpLookups
import org.apache.spark.SparkFiles
import org.apache.spark.sql.types._

object Tools {

  val schema: StructType = new StructType()
    .add("ts", StringType, nullable = true)
    .add("company", StringType, nullable = true)
    .add("device_id", StringType, nullable = true)
    .add("year", IntegerType, nullable = true)
    .add("month", IntegerType, nullable = true)
    .add("day", IntegerType, nullable = true)
    .add("hour", IntegerType, nullable = true)
    .add("minute", IntegerType, nullable = true)
    .add("second", IntegerType, nullable = true)
    .add("protocol", StringType, nullable = true)
    .add("ip_type", StringType, nullable = true)
    .add("src_mac", StringType, nullable = true)
    .add("dest_mac", StringType, nullable = true)
    .add("src_ip", StringType, nullable = true)
    .add("dest_ip", StringType, nullable = true)
    .add("src_port", IntegerType, nullable = true)
    .add("dest_port", IntegerType, nullable = true)
    .add("alert_msg", StringType, nullable = true)
    .add("classification", IntegerType, nullable = true)
    .add("priority", IntegerType, nullable = true)
    .add("sig_id", IntegerType, nullable = true)
    .add("sig_gen", IntegerType, nullable = true)
    .add("sig_rev", IntegerType, nullable = true)
    .add("src_country", StringType, nullable = true)
    .add("src_region", StringType, nullable = true)
    .add("dest_country", StringType, nullable = true)
    .add("dest_region", StringType, nullable = true)

  def validateIPAddress(ipAddress: String): Boolean = {
    val ipv4 = """^([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])\.([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])\.([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])\.([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])$""".r
    val ipv6 = """^[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}$""".r

    (ipv4 findFirstIn ipAddress)
      .map(_ => true)
      .getOrElse(
        (ipv6 findFirstIn ipAddress)
          .isDefined
      )
  }

  def IpLookupCountry(ipAddress: String): String = {
    if (!validateIPAddress(ipAddress))
      return "UNDEFINED"

    val result = (for {
      //ipLookups <- IpLookups.createFromFilenames[IO](
      ipLookups <- CreateIpLookups[IO].createFromFilenames(
        geoFile = Some(SparkFiles.get(PropertiesLoader.GeoIpFilename)),
        ispFile = None,
        domainFile = None,
        connectionTypeFile = None,
        memCache = false,
        lruCacheSize = 20000
      )

      lookup <- ipLookups.performLookups(ipAddress)
    } yield lookup).unsafeRunSync()

    result.ipLocation match {
      case Some(Right(loc)) =>
        try {
          if (loc.countryCode.isEmpty) "UNDEFINED"
          else loc.countryCode
        } catch {
          case e: NullPointerException => "UNDEFINED"
        }
      case _ =>
        "UNDEFINED"
    }
  }

  def IpLookupRegion(ipAddress: String): String = {
    if (!validateIPAddress(ipAddress))
      return "UNDEFINED"

    val result = (for {
      //ipLookups <- IpLookups.createFromFilenames[IO](
      ipLookups <- CreateIpLookups[IO].createFromFilenames(
        geoFile = Some(SparkFiles.get(PropertiesLoader.GeoIpFilename)),
        ispFile = None,
        domainFile = None,
        connectionTypeFile = None,
        memCache = false,
        lruCacheSize = 20000
      )

      lookup <- ipLookups.performLookups(ipAddress)
    } yield lookup).unsafeRunSync()

    result.ipLocation match {
      case Some(Right(loc)) =>
        try {
          if (loc.regionName.isEmpty) "UNDEFINED"
          else loc.regionName.get
        } catch {
          case e: NullPointerException => "UNDEFINED"
        }
      case _ =>
        "UNDEFINED"
    }
  }
}
