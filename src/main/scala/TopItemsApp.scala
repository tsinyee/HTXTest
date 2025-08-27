package com.htx.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TopItemsApp {

  private type AggFunc[T] = Iterable[T] => Long

  // ---------------- FUNCTIONS ----------------

  // ---------------- MAIN ----------------
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: TopItemsApp <inputA> <inputB> <output> <topX>")
      System.exit(1)
    }

    val Array(inputA, inputB, output, topXStr) = args
    val topX = topXStr.toInt

    val spark = SparkSession.builder()
      .appName("Top Items")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext

    // ---------------- READ INPUTS ----------------
    val rddA = spark.read.parquet(inputA).rdd
      .map(row => (
        row.getAs[Long]("geographical_location_oid"),
        (row.getAs[String]("item_name"), row.getAs[Long]("detection_oid"))
      ))

    val rddB = spark.read.parquet(inputB).rdd
      .map(row => (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("geographical_location")))
    
    val geoNamesMap: Map[Long, String] = rddB.collectAsMap().toMap

    // ---------------- PROCESS ----------------
    val deduped = deduplicate(rddA)
    val aggregated = aggregateByGeoId(deduped.map { case ((geoId, _), count) => (geoId, count) }, _.sum)
    val topItems = topXPerGeo(deduped, topX)
    val finalOutput = joinGeoNames(topItems, geoNamesMap)

    // ---------------- WRITE OUTPUT ----------------
    finalOutput.toDF("geographical_location_oid", "geographical_location", "item_rank", "item_name")
      .coalesce(1)
      .write.mode("overwrite")
      .parquet(output)

    spark.stop()
  }

  def deduplicate(rdd: RDD[(Long, (String, Long))]): RDD[((Long, String), Long)] =
    rdd.map { case (geoId, (item, detectionId)) => ((geoId, item, detectionId), 1) }
      .reduceByKey(_ + _)
      .map { case ((geoId, item, _), _) => ((geoId, item), 1L) }

  def aggregateByGeoId(rdd: RDD[(Long, Long)], aggFunc: AggFunc[Long]): RDD[(Long, Long)] =
    rdd.groupByKey().map { case (geoId, iter) => (geoId, aggFunc(iter)) }

  def topXPerGeo(rdd: RDD[((Long, String), Long)], topX: Int): RDD[(Long, String, String)] = {
    val geoItemCounts = rdd
      .reduceByKey(_ + _)
      .map { case ((geoId, item), count) => (geoId, (item, count)) }

    val itemsGroupedByGeo = geoItemCounts.groupByKey()

    itemsGroupedByGeo.flatMap { case (geoId, itemsIter) =>
      itemsIter.toList
        .sortBy { case (_, count) => -count }
        .take(topX)
        .zipWithIndex
        .map { case ((itemName, _), idx) => (geoId, (idx + 1).toString, itemName) }
    }
  }

  def joinGeoNames(topItems: RDD[(Long, String, String)], geoNamesMap: Map[Long, String]): RDD[(Long, String, String, String)] = {
    val bcastB = topItems.sparkContext.broadcast(geoNamesMap)
    topItems.map { case (geoId, rank, item) =>
      val geoName = bcastB.value.getOrElse(geoId, "UNKNOWN")
      (geoId, geoName, rank, item)
    }
  }
}
