package gcp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

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
      .textFile("gs://sgu_input_dataset/*")

    ds
      .flatMap(x => x.split(" "))
      .groupByKey(x => x)
      .count()
      .withColumnRenamed("count(1)", "total")
      .orderBy($"total".desc)
      .show()
  }
}