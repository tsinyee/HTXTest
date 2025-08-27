package com.htx.spark.test

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TopItemsAppUnitTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var sc: org.apache.spark.SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("UnitTest")
      .getOrCreate()
    sc = spark.sparkContext
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  test("deduplicate should remove duplicates and count 1") {
    val rddA = sc.parallelize(Seq(
      (1001L, ("Apple", 1L)),
      (1001L, ("Apple", 1L)),
      (1001L, ("Banana", 2L))
    ))

    val dedupedA = TopItemsApp.deduplicate(rddA)

    val expected = Set(
      ((1001L, "Apple"), 1L),
      ((1001L, "Banana"), 1L)
    )

    assert(dedupedA.collect().toSet == expected)
  }

  test("aggregateByGeoId should sum counts correctly") {
    val rdd = sc.parallelize(Seq(
      (1001L, 1L),
      (1001L, 2L),
      (1002L, 3L)
    ))

    val result = TopItemsApp.aggregateByGeoId(rdd, _.sum).collect().toSet
    val expected = Set(
      (1001L, 3L),
      (1002L, 3L)
    )

    assert(result == expected)
  }

  test("topXPerGeo should return correct top items") {
    val dedupedA = sc.parallelize(Seq(
      ((1001L, "Apple"), 3L),
      ((1001L, "Banana"), 2L),
      ((1001L, "Cherry"), 1L),
      ((1002L, "Durian"), 2L),
      ((1002L, "Eggfruit"), 1L)
    ))

    val topX = 2
    val topItems = TopItemsApp.topXPerGeo(dedupedA, topX).collect().toSet

    val expected = Set(
      (1001L, "1", "Apple"),
      (1001L, "2", "Banana"),
      (1002L, "1", "Durian"),
      (1002L, "2", "Eggfruit")
    )

    assert(topItems == expected)
  }

  test("joinGeoNames should correctly join top items with geo names") {
    val topItems = sc.parallelize(Seq(
      (1001L, "1", "Apple"),
      (1001L, "2", "Banana"),
      (1002L, "1", "Durian")
    ))

    val geoNamesMap = Map(
      1001L -> "Geo-A",
      1002L -> "Geo-B"
    )

    val finalOutput = TopItemsApp.joinGeoNames(topItems, geoNamesMap).collect().toSet

    val expected = Set(
      (1001L, "Geo-A", "1", "Apple"),
      (1001L, "Geo-A", "2", "Banana"),
      (1002L, "Geo-B", "1", "Durian")
    )

    assert(finalOutput == expected)
  }
}
