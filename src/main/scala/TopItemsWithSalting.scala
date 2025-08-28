package com.htx.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random

object TopItemsWithSalting {
  
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: TopItemsWithSalting <inputA> <inputB> <output> <topX>")
      System.exit(1)
    }

    val inputA = args(0)
    val inputB = args(1)
    val output = args(2)
    val topX = args(3).toInt

    val spark = SparkSession.builder()
      .appName("Top Items Skew Optimised")
      .master("local[*]")
      .getOrCreate()

    // READ INPUTS
    val rddA = spark.read.parquet(inputA).rdd
      .map(row => (row.getAs[Long]("geographical_location_oid"), (row.getAs[String]("item_name"), row.getAs[Long]("detection_oid"))))
    val rddB = spark.read.parquet(inputB).rdd
      .map(row => (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("geographical_location")))
    val geoNamesMap = rddB.collectAsMap().toMap

    // PROCESS
    val dedupedA = deduplicate(rddA)
    val skewedGeoIds = detectSkew(dedupedA, threshold = 10000L)
    val salted = applySalting(dedupedA, skewedGeoIds)
    val aggregated = aggregatePerGeo(salted)
    val topItems = topXPerGeo(aggregated, topX)
    val finalOutput = joinGeoNames(topItems, geoNamesMap)

    // WRITE OUTPUT
    import spark.implicits._
    finalOutput.toDF("geographical_location_oid", "geographical_location", "item_rank", "item_name")
      .coalesce(1)
      .write.mode("overwrite")
      .parquet(output)

    spark.stop()
  }

  def deduplicate(rdd: RDD[(Long, (String, Long))]): RDD[((Long, String), Long)] =
    rdd.map { case (geoId, (item, detId)) => ((geoId, item, detId), 1) }
      .reduceByKey(_ + _)
      .map { case ((geoId, item, _), _) => ((geoId, item), 1L) }

  def detectSkew(rdd: RDD[((Long, String), Long)], threshold: Long): Set[Long] =
    rdd.map { case ((geoId, _), _) => (geoId, 1L) }
      .reduceByKey(_ + _)
      .filter(_._2 > threshold)
      .keys.collect().toSet

  def applySalting(rdd: RDD[((Long, String), Long)], skewed: Set[Long]): RDD[((Long, Int), (String, Long))] = {
    val broadcastSkewed = rdd.sparkContext.broadcast(skewed)
    rdd.map { case ((geoId, item), count) =>
      val salt = if (broadcastSkewed.value.contains(geoId)) Random.nextInt(10) else 0
      ((geoId, salt), (item, count))
    }
  }

  def aggregatePerGeo(rdd: RDD[((Long, Int), (String, Long))]): RDD[(Long, Map[String, Long])] = {
    val aggregatedSalted = rdd
      .mapValues { case (item, count) => Map(item -> count) }
      .reduceByKey { (map1, map2) =>
        (map1.keySet ++ map2.keySet).map(k => k -> (map1.getOrElse(k, 0L) + map2.getOrElse(k, 0L))).toMap
      }

    aggregatedSalted
      .map { case ((geoId, _), itemMap) => (geoId, itemMap) }
      .reduceByKey { (map1, map2) =>
        (map1.keySet ++ map2.keySet).map(k => k -> (map1.getOrElse(k, 0L) + map2.getOrElse(k, 0L))).toMap
      }
  }

  def topXPerGeo(rdd: RDD[(Long, Map[String, Long])], topX: Int): RDD[(Long, String, String)] =
    rdd.flatMap { case (geoId, itemMap) =>
      itemMap.toList
        // tie-breaker: sort by item name if counts are equal
        .sortBy { case (itemName, count) => (-count, itemName) }
        .take(topX)
        .zipWithIndex
        .map { case ((itemName, _), idx) =>
          (geoId, (idx + 1).toString, itemName)
        }
    }

  def joinGeoNames(topX: RDD[(Long, String, String)], geoNames: Map[Long, String]): RDD[(Long, String, String, String)] = {
    val bcastB = topX.sparkContext.broadcast(geoNames)
    topX.map { case (geoId, rank, item) =>
      val geoName = bcastB.value.getOrElse(geoId, "UNKNOWN")
      (geoId, geoName, rank, item)
    }
  }
}
