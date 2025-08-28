package com.htx.spark.test

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TopItemsAppE2ETest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var sc: org.apache.spark.SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("E2ETest")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    sc = spark.sparkContext
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }

  test("end-to-end: deduplicate, aggregate, topX, join geo names with intermediate assertions") {
    val rddA = sc.parallelize(Seq(
      (1001L, ("Apple", 1L)),
      (1001L, ("Apple", 2L)),
      (1001L, ("Banana", 3L)),
      (1001L, ("Banana", 3L)),
      (1002L, ("Cherry", 4L)),
      (1002L, ("Durian", 5L)),
      (1002L, ("Eggfruit", 6L))
    ))

    val rddB = sc.parallelize(Seq(
      (1001L, "Geo-A"),
      (1002L, "Geo-B")
    ))
    val geoNamesMap = rddB.collectAsMap().toMap

    val deduped = TopItemsApp.deduplicate(rddA)
    val dedupedExpected = Set(
      ((1001L, "Apple"), 1L),
      ((1001L, "Apple"), 1L),
      ((1001L, "Banana"), 1L),
      ((1002L, "Cherry"), 1L),
      ((1002L, "Durian"), 1L),
      ((1002L, "Eggfruit"), 1L)
    )
    assert(deduped.collect().toSet == dedupedExpected)

    val aggregated = TopItemsApp.aggregateByGeo(deduped, _.sum)
    val aggregatedExpected = Set(
      ((1001L, "Apple"), 2L),
      ((1001L, "Banana"), 1L),
      ((1002L, "Cherry"), 1L),
      ((1002L, "Durian"), 1L),
      ((1002L, "Eggfruit"), 1L)
    )
    assert(aggregated.collect().toSet == aggregatedExpected)

    val topItems = TopItemsApp.topXPerGeo(aggregated, topX = 2)
    val topItemsExpected = Set(
      (1001L, "1", "Apple"),
      (1001L, "2", "Banana"),
      (1002L, "1", "Cherry"),
      (1002L, "2", "Durian")
    )
    assert(topItems.collect().toSet == topItemsExpected)

    val finalOutput = TopItemsApp.joinGeoNames(topItems, geoNamesMap)
    val expectedFinal = Set(
      (1001L, "Geo-A", "1", "Apple"),
      (1001L, "Geo-A", "2", "Banana"),
      (1002L, "Geo-B", "1", "Cherry"),
      (1002L, "Geo-B", "2", "Durian")
    )
    assert(finalOutput.collect().toSet == expectedFinal)
  }
}
