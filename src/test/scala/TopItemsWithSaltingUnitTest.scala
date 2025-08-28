package com.htx.spark.test

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TopItemsWithSaltingUnitTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var sc: org.apache.spark.SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("SaltingUnitTest")
      .getOrCreate()
    sc = spark.sparkContext
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      super.afterAll()
    }
  }


  test("deduplicate should remove duplicates") {
    val rddA = sc.parallelize(Seq(
      (1001L, ("Apple", 1L)),
      (1001L, ("Apple", 1L)),
      (1001L, ("Banana", 2L))
    ))

    val deduped = TopItemsWithSalting.deduplicate(rddA)
    val expected = Set(
      ((1001L, "Apple"), 1L),
      ((1001L, "Banana"), 1L)
    )
    assert(deduped.collect().toSet == expected)
  }

  test("detectSkew should identify geoIds above threshold") {
    val deduped = sc.parallelize(Seq(
      ((1001L, "Apple"), 1L),
      ((1001L, "Banana"), 1L),
      ((1002L, "Cherry"), 1L)
    ))
    val skewed = TopItemsWithSalting.detectSkew(deduped, threshold = 1L)
    assert(skewed == Set(1001L))
  }

  test("applySalting should add salt for skewed geoIds") {
    val deduped = sc.parallelize(Seq(
      ((1001L, "Apple"), 1L),
      ((1002L, "Banana"), 1L)
    ))
    val skewed = Set(1001L)
    val salted = TopItemsWithSalting.applySalting(deduped, skewed).collect()
    // Geo 1001 should have salt in 0-9, geo 1002 should have 0
    assert(salted.exists { case ((geoId, salt), _) => geoId == 1001L && salt >= 0 && salt <= 9 })
    assert(salted.exists { case ((geoId, salt), _) => geoId == 1002L && salt == 0 })
  }

  test("aggregatePerGeo should sum counts correctly") {
    val salted = sc.parallelize(Seq(
      ((1001L, 0), ("Apple", 2L)),
      ((1001L, 0), ("Banana", 3L)),
      ((1002L, 0), ("Cherry", 1L))
    ))
    val aggregated = TopItemsWithSalting.aggregatePerGeo(salted).collect().toSet
    val expected = Set(
      (1001L, Map("Apple" -> 2L, "Banana" -> 3L)),
      (1002L, Map("Cherry" -> 1L))
    )
    assert(aggregated == expected)
  }

  test("topXPerGeo should select top X items") {
    val aggregated = sc.parallelize(Seq(
      (1001L, Map("Apple" -> 5L, "Banana" -> 3L, "Cherry" -> 1L)),
      (1002L, Map("Durian" -> 2L, "Eggfruit" -> 1L))
    ))
    val topX = 2
    val topItems = TopItemsWithSalting.topXPerGeo(aggregated, topX).collect().toSet
    val expected = Set(
      (1001L, "1", "Apple"),
      (1001L, "2", "Banana"),
      (1002L, "1", "Durian"),
      (1002L, "2", "Eggfruit")
    )
    assert(topItems == expected)
  }

  test("joinGeoNames should correctly attach geo names") {
    val topItems = sc.parallelize(Seq(
      (1001L, "1", "Apple"),
      (1001L, "2", "Banana"),
      (1002L, "1", "Durian")
    ))
    val geoNames = Map(1001L -> "Geo-A", 1002L -> "Geo-B")
    val finalOutput = TopItemsWithSalting.joinGeoNames(topItems, geoNames).collect().toSet
    val expected = Set(
      (1001L, "Geo-A", "1", "Apple"),
      (1001L, "Geo-A", "2", "Banana"),
      (1002L, "Geo-B", "1", "Durian")
    )
    assert(finalOutput == expected)
  }

  test("end-to-end pipeline") {
    val rddA = sc.parallelize(Seq(
      (1001L, ("Apple", 1L)),
      (1001L, ("Apple", 1L)),
      (1001L, ("Banana", 2L)),
      (1002L, ("Durian", 3L))
    ))
    val geoNames = Map(1001L -> "Geo-A", 1002L -> "Geo-B")

    val deduped = TopItemsWithSalting.deduplicate(rddA)
    val skewed = TopItemsWithSalting.detectSkew(deduped, threshold = 1L)
    val salted = TopItemsWithSalting.applySalting(deduped, skewed)
    val aggregated = TopItemsWithSalting.aggregatePerGeo(salted)
    val topItems = TopItemsWithSalting.topXPerGeo(aggregated, topX = 2)
    val finalOutput = TopItemsWithSalting.joinGeoNames(topItems, geoNames).collect().toSet

    val expected = Set(
      (1001L, "Geo-A", "1", "Apple"),
      (1001L, "Geo-A", "2", "Banana"),
      (1002L, "Geo-B", "1", "Durian")
    )
    assert(finalOutput == expected)
  }
}

