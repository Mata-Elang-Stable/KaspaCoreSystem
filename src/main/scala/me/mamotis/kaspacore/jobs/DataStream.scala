package me.mamotis.kaspacore.jobs

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import me.mamotis.kaspacore.util._
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.joda.time.DateTime

import java.sql.Timestamp

object DataStream extends Utils {

  def main(args: Array[String]): Unit = {

    //=================================SPARK CONFIGURATION==================================
    val sparkSession = getSparkSession(args)
    val sparkContext = getSparkContext(sparkSession)

    //=================================AVRO DESERIALIZER====================================
    val utils = new ConfluentSparkAvroUtils(PropertiesLoader.schemaRegistryUrl)
    //    val keyDes = utils.deserializerForSubject(PropertiesLoader.kafkaInputTopic + "-key")
    val valDes = utils.deserializerForSubject(PropertiesLoader.kafkaInputTopic + "-value")

    // Maxmind GeoIP Configuration
    sparkContext.addFile(PropertiesLoader.GeoIpPath)

    // Cassandra Connector
    val connector = getCassandraSession(sparkContext)

    // set implicit and log level
    import sparkSession.implicits._
    sparkContext.setLogLevel("ERROR")

    //==================================KAFKA DEFINITION=====================================

    val kafkaStreamDF = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertiesLoader.kafkaBrokerUrl)
      .option("subscribe", PropertiesLoader.kafkaInputTopic)
      .option("startingOffsets", PropertiesLoader.kafkaStartingOffset)
      .load()

    val decoded = kafkaStreamDF.select(
      valDes(col("value")).alias("value")
    )

    val parsedRawDf = decoded.select("value.*")

    //======================================DATAFRAME PARSING==================================

    //region +++++++++++Raw Data++++++++++++++

    val eventDf = parsedRawDf.select(
      $"timestamp", $"device_id", $"protocol", $"ip_type", $"src_mac", $"dest_mac", $"src_ip",
      $"dest_ip", $"src_port", $"dst_port", $"alert_msg", $"classification", $"priority", $"sig_id",
      $"sig_gen", $"sig_rev", $"company"
    ).map { r =>
      val ts = r.getAs[String](0)
      val device_id = r.getAs[String](1)
      val protocol = r.getAs[String](2)
      val ip_type = r.getAs[String](3)
      val src_mac = r.getAs[String](4)
      val dest_mac = r.getAs[String](5)
      val src_ip = r.getAs[String](6)
      val dest_ip = r.getAs[String](7)
      val src_port = r.getAs[Long](8).toInt
      val dest_port = r.getAs[Long](9).toInt
      val alert_msg = r.getAs[String](10)
      val classification = r.getAs[Long](11).toInt
      val priority = r.getAs[Long](12).toInt
      val sig_id = r.getAs[Long](13).toInt
      val sig_gen = r.getAs[Long](14).toInt
      val sig_rev = r.getAs[Long](15).toInt
      val company = r.getAs[String](16)
      val src_country = Tools.IpLookupCountry(src_ip)
      val src_region = Tools.IpLookupRegion(src_ip)
      val dest_country = Tools.IpLookupCountry(dest_ip)
      val dest_region = Tools.IpLookupRegion(dest_ip)

      val date = new DateTime((ts.toDouble * 1000).toLong)

      Commons.EventObj(
        ts, company, device_id, date.year().get(), date.monthOfYear().get(), date.dayOfMonth().get(),
        date.hourOfDay().get(), date.minuteOfHour().get(), date.secondOfMinute().get(),
        protocol, ip_type, src_mac, dest_mac, src_ip, dest_ip,
        src_port, dest_port, alert_msg, classification, priority,
        sig_id, sig_gen, sig_rev, src_country, src_region, dest_country, dest_region
      )
    }.toDF(ColsArtifact.colsEventObj: _*)

    val eventDs = eventDf.select($"ts", $"company", $"device_id", $"year", $"month",
      $"day", $"hour", $"minute", $"second", $"protocol", $"ip_type",
      $"src_mac", $"dest_mac", $"src_ip", $"dest_ip", $"src_port",
      $"dest_port", $"alert_msg", $"classification", $"priority",
      $"sig_id", $"sig_gen", $"sig_rev", $"src_country", $"src_region",
      $"dest_country", $"dest_region").as[Commons.EventObj]

    //++++++++++++++++++++++++Event for Dashboard++++++++++++++++++++++++
    //+++++Second
    val eventDf1s = parsedRawDf.select(
      to_utc_timestamp(from_unixtime($"timestamp"), "GMT").alias("timestamp"),
      $"device_id", $"protocol", $"ip_type", $"src_mac", $"dest_mac", $"src_ip",
      $"dest_ip", $"src_port", $"dst_port", $"alert_msg", $"classification", $"priority", $"sig_id",
      $"sig_gen", $"sig_rev", $"company"
    ).withColumn(
      "value", lit(1)
    ).withWatermark(
      "timestamp", "1 seconds"
    ).groupBy(
      $"device_id", $"protocol", $"ip_type", $"src_mac", $"dest_mac", $"src_ip",
      $"dest_ip", $"src_port", $"dst_port", $"alert_msg", $"classification", $"priority", $"sig_id",
      $"sig_gen", $"sig_rev", $"company",
      window($"timestamp", "1 seconds").alias("timestamp")
    ).sum("value")

    val eventDf1s_2 = eventDf1s.select(
      $"timestamp.start", $"device_id", $"protocol", $"ip_type", $"src_mac", $"dest_mac", $"src_ip",
      $"dest_ip", $"src_port", $"dst_port", $"alert_msg", $"classification", $"priority", $"sig_id",
      $"sig_gen", $"sig_rev", $"company", $"sum(value)"
    ).map {
      r =>
        val ts = r.getAs[Timestamp](0).toString
        val device_id = r.getAs[String](1)
        val protocol = r.getAs[String](2)
        val ip_type = r.getAs[String](3)
        val src_mac = r.getAs[String](4)
        val dest_mac = r.getAs[String](5)
        val src_ip = r.getAs[String](6)
        val dest_ip = r.getAs[String](7)
        val src_port = r.getAs[Long](8).toInt
        val dest_port = r.getAs[Long](9).toInt
        val alert_msg = r.getAs[String](10)
        val classification = r.getAs[Long](11).toInt
        val priority = r.getAs[Long](12).toInt
        val sig_id = r.getAs[Long](13).toInt
        val sig_gen = r.getAs[Long](14).toInt
        val sig_rev = r.getAs[Long](15).toInt
        val company = r.getAs[String](16)
        val value = r.getAs[Long](17)

        val src_country = Tools.IpLookupCountry(src_ip)
        val src_region = Tools.IpLookupRegion(src_ip)
        val dest_country = Tools.IpLookupCountry(dest_ip)
        val dest_region = Tools.IpLookupRegion(dest_ip)


        Commons.EventObj1s(
          ts, company, device_id, protocol, ip_type, src_mac, dest_mac, src_ip, dest_ip, src_port, dest_port,
          alert_msg, classification, priority, sig_id, sig_gen, sig_rev, src_country, src_region,
          dest_country, dest_region, value
        )
    }.toDF(ColsArtifact.colsEventObj1s: _*)

    val eventDs1s = eventDf1s_2.select(
      $"timestamp", $"company", $"device_id", $"protocol", $"ip_type", $"src_mac", $"dest_mac",
      $"src_ip", $"dest_ip", $"src_port", $"dest_port", $"alert_msg", $"classification", $"priority",
      $"sig_id", $"sig_gen", $"sig_rev", $"src_country", $"src_region", $"dest_country", $"dest_region", $"value"
    ).as[Commons.EventObj1s]

    //+++++Minute
    // TODO:

    //+++++Hour
    // TODO:

    //+++++++++++++Push Event Hit Company per Second++++++++++++++++++++++
    //+++++Second
    val eventHitCompanySecDf_1 = parsedRawDf.select(
      to_utc_timestamp(from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType),
      $"company"
    ).withColumn("value", lit(1)
    ).groupBy(
      $"company",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val eventHitCompanySecDf_2 = eventHitCompanySecDf_1.select($"company", $"windows.start", $"sum(value)").map {
      r =>
        val company = r.getAs[String](0)

        val epoch = r.getAs[Timestamp](1).getTime

        val date = new DateTime(epoch)
        val value = r.getAs[Long](2)

        Commons.EventHitCompanyObjSec(
          company, date.year().get(), date.monthOfYear().get(), date.dayOfMonth().get(), date.hourOfDay().get(),
          date.minuteOfHour().get(), date.secondOfMinute().get(), value
        )
    }.toDF(ColsArtifact.colsEventHitCompanyObjSec: _*)

    val eventHitCompanySecDs = eventHitCompanySecDf_2.select($"company", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.EventHitCompanyObjSec]

    //+++++Minute
    val eventHitCompanyMinDf_1 = parsedRawDf.select(
      to_utc_timestamp(from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType),
      $"company"
    ).withColumn("value", lit(1)
    ).groupBy(
      $"company",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val eventHitCompanyMinDf_2 = eventHitCompanyMinDf_1.select($"company", $"windows.start", $"sum(value)").map {
      r =>
        val company = r.getAs[String](0)
        val epoch = r.getAs[Timestamp](1).getTime
        val date = new DateTime(epoch)
        val value = r.getAs[Long](2)

        Commons.EventHitCompanyObjMin(
          company, date.year().get(), date.monthOfYear().get(), date.dayOfMonth().get(), date.hourOfDay().get(), date.minuteOfHour().get(), value
        )
    }.toDF(ColsArtifact.colsEventHitCompanyObjMin: _*)

    val eventHitCompanyMinDs = eventHitCompanyMinDf_2.select($"company", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.EventHitCompanyObjMin]

    //+++++Hour
    val eventHitCompanyHourDf_1 = parsedRawDf.select(
      to_utc_timestamp(from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType),
      $"company"
    ).withColumn("value", lit(1)
    ).groupBy(
      $"company",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val eventHitCompanyHourDf_2 = eventHitCompanyHourDf_1
      .select($"company", $"windows.start", $"sum(value)")
      .map {
        r =>
          val company = r.getAs[String](0)

          val epoch = r.getAs[Timestamp](1).getTime

          val date = new DateTime(epoch)

          val value = r.getAs[Long](2)

          Commons.EventHitCompanyObjHour(
            company, date.year().get(), date.monthOfYear().get(), date.dayOfMonth().get(), date.hourOfDay().get(), value
          )
      }.toDF(ColsArtifact.colsEventHitCompanyObjHour: _*)

    val eventHitCompanyHourDs = eventHitCompanyHourDf_2.select($"company", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.EventHitCompanyObjHour]

    //+++++++++++++Push Event Hit DeviceId per Second++++++++++++++++++++++
    //+++++Second
    val eventHitDeviceIdSecDf_1 = parsedRawDf.select(
      to_utc_timestamp(from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType),
      $"device_id"
    ).withColumn("value", lit(1)
    ).groupBy(
      $"device_id",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val eventHitDeviceIdSecDf_2 = eventHitDeviceIdSecDf_1.select(
      $"device_id", $"windows.start", $"sum(value)"
    ).map {
      r =>
        val device_id = r.getAs[String](0)

        val epoch = r.getAs[Timestamp](1).getTime

        val date = new DateTime(epoch)

        val value = r.getAs[Long](2)

        Commons.EventHitDeviceIdObjSec(
          device_id, date.year().get(), date.monthOfYear().get(), date.dayOfMonth().get(), date.hourOfDay().get(),
          date.minuteOfHour().get(), date.secondOfMinute().get(), value
        )
    }.toDF(ColsArtifact.colsEventHitDeviceIdObjSec: _*)

    val eventHitDeviceIdSecDs = eventHitDeviceIdSecDf_2.select($"device_id", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.EventHitDeviceIdObjSec]

    //+++++Minute
    val eventHitDeviceIdMinDf_1 = parsedRawDf.select(
      to_utc_timestamp(from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType),
      $"device_id"
    ).withColumn("value", lit(1)
    ).groupBy(
      $"device_id",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val eventHitDeviceIdMinDf_2 = eventHitDeviceIdMinDf_1
      .select($"device_id", $"windows.start", $"sum(value)")
      .map {
        r =>
          val device_id = r.getAs[String](0)

          val epoch = r.getAs[Timestamp](1).getTime

          val date = new DateTime(epoch)

          val value = r.getAs[Long](2)

          Commons.EventHitDeviceIdObjMin(
            device_id, date.year().get(), date.monthOfYear().get(), date.dayOfMonth().get(), date.hourOfDay().get(), date.minuteOfHour().get(), value
          )
      }.toDF(ColsArtifact.colsEventHitDeviceIdObjMin: _*)

    val eventHitDeviceIdMinDs = eventHitDeviceIdMinDf_2.select($"device_id", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.EventHitDeviceIdObjMin]

    //+++++Hour
    val eventHitDeviceIdHourDf_1 = parsedRawDf.select(
      to_utc_timestamp(from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType),
      $"device_id"
    ).withColumn("value", lit(1)
    ).groupBy(
      $"device_id",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val eventHitDeviceIdHourDf_2 = eventHitDeviceIdHourDf_1
      .select($"device_id", $"windows.start", $"sum(value)")
      .map {
        r =>
          val device_id = r.getAs[String](0)
          val epoch = r.getAs[Timestamp](1).getTime
          val date = new DateTime(epoch)
          val value = r.getAs[Long](2)

          Commons.EventHitDeviceIdObjHour(
            device_id, date.year().get(), date.monthOfYear().get(),
            date.dayOfMonth().get(), date.hourOfDay().get(), value
          )
      }.toDF(ColsArtifact.colsEventHitDeviceIdObjHour: _*)

    val eventHitDeviceIdHourDs = eventHitDeviceIdHourDf_2.select($"device_id", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.EventHitDeviceIdObjHour]

    //endregion

    //======================================================CASSANDRA WRITER======================================
    val writerEventHitCompanySec: ForeachWriter[Commons.EventHitCompanyObjSec] =
      new ForeachWriter[Commons.EventHitCompanyObjSec] {
        override def open(partitionId: Long, version: Long): Boolean = true

        override def process(value: Commons.EventHitCompanyObjSec): Unit = {
          PushArtifact.pushEventHitCompanySec(value, connector)
        }

        override def close(errorOrNull: Throwable): Unit = {}
      }

    val writerEventHitCompanyMin = new ForeachWriter[Commons.EventHitCompanyObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitCompanyObjMin): Unit = {
        PushArtifact.pushEventHitCompanyMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitCompanyHour = new ForeachWriter[Commons.EventHitCompanyObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitCompanyObjHour): Unit = {
        PushArtifact.pushEventHitCompanyHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitDeviceIdSec = new ForeachWriter[Commons.EventHitDeviceIdObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitDeviceIdObjSec): Unit = {
        PushArtifact.pushEventHitDeviceIdSec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitDeviceIdMin = new ForeachWriter[Commons.EventHitDeviceIdObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitDeviceIdObjMin): Unit = {
        PushArtifact.pushEventHitDeviceIdMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitDeviceIdHour = new ForeachWriter[Commons.EventHitDeviceIdObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitDeviceIdObjHour): Unit = {
        PushArtifact.pushEventHitDeviceIdHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    //====================================================WRITE QUERY=================================

    eventDs1s.selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", PropertiesLoader.kafkaBrokerUrlOutput)
      .option("topic", PropertiesLoader.kafkaEvent1sOutputTopic)
      .start()

    eventDs
      .writeStream
      .format("json")
      .option("path", PropertiesLoader.hadoopEventFilePath)
      .option("checkpointLocation", PropertiesLoader.checkpointLocation)
      .start()

    eventHitCompanySecDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitCompanyPerSec")
      .foreach(writerEventHitCompanySec)
      .start()

    eventHitCompanyMinDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitCompanyPerMin")
      .foreach(writerEventHitCompanyMin)
      .start()

    eventHitCompanyHourDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitCompanyPerHour")
      .foreach(writerEventHitCompanyHour)
      .start()

    eventHitDeviceIdSecDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitDeviceIdPerSec")
      .foreach(writerEventHitDeviceIdSec)
      .start()

    eventHitDeviceIdMinDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitDeviceIdPerMin")
      .foreach(writerEventHitDeviceIdMin)
      .start()

    eventHitDeviceIdHourDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitDeviceIdPerHour")
      .foreach(writerEventHitDeviceIdHour)
      .start()

    sparkSession.streams.awaitAnyTermination()
  }

}
