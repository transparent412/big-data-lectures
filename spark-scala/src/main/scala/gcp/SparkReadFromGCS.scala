package gcp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkReadFromGCS {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)

    val ss = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    import ss.implicits._
    val ds = ss
      .read
      .option("header",true)
      .csv("gs://podborodok/*")

    ds

      .select($"country",$"state").filter(ds("state")==="failed")
      .groupBy($"country").count().orderBy($"count".desc)
      //.write.format("bigquery").option("table","statesucces.ABC")
      ds.write
      .format("bigquery")
      .option("table", "statesucces.pub_sub_messages")
      .mode(SaveMode.Append)
      .save()
    ds
      //.flatMap(x => x.split(" "))
      .select($"country",$"state").filter(ds("state")==="successful")
      .groupBy($"country").count().orderBy($"count".desc).write.format("bigquery").option("table","Top 10 countries by the number of successful projects")
      ds
      .select($"name",$"state").filter(ds("state")==="successful"||ds("state")==="failed")
     .groupBy( $"state").count().write.format("bigquery").option("table","Number of successful and failed projects")

  }
}