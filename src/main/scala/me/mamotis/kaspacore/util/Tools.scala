package me.mamotis.kaspacore.util
import org.apache.spark.sql.types._

import cats.effect.IO
import com.snowplowanalytics.maxmind.iplookups.IpLookups
import org.apache.spark.SparkFiles

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

  def IpLookupCountry(ipAddress: String): String = {
    val result = (for {
      ipLookups <- IpLookups.createFromFilenames[IO](
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
        if(loc.countryName.isEmpty) "UNDEFINED"
        else loc.countryName
      case _ =>
        "UNDEFINED"
    }
  }

  def IpLookupRegion(ipAddress: String): String = {
    val result = (for {
      ipLookups <- IpLookups.createFromFilenames[IO](
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
        if(loc.regionName.isEmpty) "UNDEFINED"
        else loc.regionName.get
      case _ =>
        "UNDEFINED"
    }
  }
}
