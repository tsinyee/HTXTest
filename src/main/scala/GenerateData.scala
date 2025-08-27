package com.htx.spark.test

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.util.Random

object GenerateData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Generate Dataset A and Bwat")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // ================= Dataset A =================
    val schemaA = StructType(Seq(
      StructField("geographical_location_oid", LongType, nullable = false),
      StructField("video_camera_oid", LongType, nullable = false),
      StructField("detection_oid", LongType, nullable = false),
      StructField("item_name", StringType, nullable = false),
      StructField("timestamp_detected", LongType, nullable = false)
    ))

    val numRowsA = 100000 // for testing, 100k rows
    val random = new Random()

    val rowsA = sc.parallelize(1 to numRowsA).map { i =>
      Row(
        random.nextInt(10).toLong + 1, // geographical_location_oid 1..10
        random.nextInt(50).toLong + 1, // video_camera_oid 1..50
        i.toLong, // detection_oid unique
        s"item_${random.nextInt(100)}", // item_name
        1690000000L + random.nextInt(100000) // timestamp_detected
      )
    }

    val dfA = spark.createDataFrame(rowsA, schemaA)
    dfA.write.mode(SaveMode.Overwrite).parquet("src/test/resources/datasetA.parquet")

    // ================= Dataset B =================
    import spark.implicits._

    val dfB = Seq(
      (1L, "Downtown"),
      (2L, "Uptown"),
      (3L, "Midtown"),
      (4L, "Suburb"),
      (5L, "Riverside"),
      (6L, "Industrial"),
      (7L, "Harbor"),
      (8L, "Eastside"),
      (9L, "Westside"),
      (10L, "Central")
    ).toDF("geographical_location_oid", "geographical_location")

    dfB.write.mode(SaveMode.Overwrite).parquet("src/test/resources/datasetB.parquet")

    println("Sample Dataset A & B Parquet files generated successfully!")
    spark.stop()
  }
}
