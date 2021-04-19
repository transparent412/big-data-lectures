package gcp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.nio.charset.StandardCharsets
import java.sql.Timestamp

object SparkDemoDStreamPubSub {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)

    import org.apache.spark._

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))

    val projectID = "ethereal-zodiac-309808"
    val subscription = "test-sub"

    val lines = PubsubUtils.createStream(
        ssc,
        projectID,
        Some("test"),
        subscription, // Cloud Pub/Sub subscription for incoming tweets
        SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))

//    lines.print()

    val sc = SparkSession.builder()
      .master("local[*]")
      .appName("pub/sub streaming")
      .getOrCreate()

    val schema = StructType(
      Seq(
        StructField(name = "message", dataType = StringType, nullable = false),
        StructField(name = "load_timestamp", dataType = TimestampType, nullable = false),
      )
    )

    lines.foreachRDD(rdd => {
      val current_timestamp = new Timestamp(System.currentTimeMillis())

      val rows = rdd.map(l => Row(l, current_timestamp))

      val df = sc.createDataFrame(rows, schema)
      df.write
        .format("bigquery")
        .option("table", "database.pub_sub_messages")
        .option("temporaryGcsBucket", "sgu_bigquery_tmp")
        .mode(SaveMode.Append)
        .save()
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}