package com.htx.spark.test

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TopItemsWithSaltingE2ETest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var sc: org.apache.spark.SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("TopItemsWithSaltingE2E")
      // make small/local runs faster & deterministic-ish
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    sc = spark.sparkContext
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }

  test("end-to-end with intermediate checks (dedupe → skew → salt → aggregate → topX → join)") {
    // Input A: (geoId, (itemName, detectionId))
    val rddA = sc.parallelize(Seq(
      (1001L, ("Apple", 1L)),
      (1001L, ("Apple", 2L)),
      (1001L, ("Banana", 3L)),
      (1001L, ("Banana", 4L)),
      (1001L, ("Cherry", 5L)),
      (1002L, ("Durian", 6L)),
      (1002L, ("Durian", 7L)),
      (1002L, ("Eggfruit", 8L))
    ))

    val geoNamesMap = Map(
      1001L -> "Geo-A",
      1002L -> "Geo-B"
    )

    // 1) Deduplicate per (geoId,item,detectionId) → ((geoId,item), 1L)
    val deduped = TopItemsWithSalting.deduplicate(rddA)
    val dedupedExpected = Set(
      ((1001L, "Apple"), 1L),
      ((1001L, "Banana"), 1L),
      ((1001L, "Cherry"), 1L),
      ((1002L, "Durian"), 1L),
      ((1002L, "Eggfruit"), 1L)
    )
    assert(deduped.collect().toSet == dedupedExpected)

    // 2) Detect skew PER GEO by counting rows per geo (not sums per item)
    val skewedGeoIds = TopItemsWithSalting.detectSkew(deduped, threshold = 3L)
    assert(skewedGeoIds == Set(1001L))

    // 3) Apply salting (skewed geos get salt in 0..9; non-skew geo salt=0)
    val salted = TopItemsWithSalting.applySalting(deduped, skewedGeoIds)

    // Validate geoIds preserved
    val saltedGeoIds = salted.map(_._1._1).distinct().collect().toSet
    assert(saltedGeoIds == Set(1001L, 1002L))

    // Validate salt domain: skewed in 0..9, non-skewed exactly {0}
    val salts1001 = salted.filter(_._1._1 == 1001L).map(_._1._2).distinct().collect().toSet
    val salts1002 = salted.filter(_._1._1 == 1002L).map(_._1._2).distinct().collect().toSet
    assert(salts1001.subsetOf((0 to 9).toSet))
    assert(salts1002 == Set(0))

    // 4) Aggregate back per geo (remove salt, sum item counts)
    val aggregated = TopItemsWithSalting.aggregatePerGeo(salted)
    val aggregatedMap = aggregated.collect().toMap
    assert(aggregatedMap(1001L) == Map("Apple" -> 2L, "Banana" -> 2L, "Cherry" -> 1L))
    assert(aggregatedMap(1002L) == Map("Durian" -> 2L, "Eggfruit" -> 1L))

    // 5) Top-X per geo (with tie-breaker by item name)
    val topItems = TopItemsWithSalting.topXPerGeo(aggregated, topX = 1)
    val topItemsExpected = Set(
      // Apple(2) vs Banana(2): Apple wins alphabetically
      (1001L, "1", "Apple"),
      (1002L, "1", "Durian")
    )
    assert(topItems.collect().toSet == topItemsExpected)

    // 6) Join with geo names
    val finalOutput = TopItemsWithSalting.joinGeoNames(topItems, geoNamesMap)
    val expectedFinal = Set(
      (1001L, "Geo-A", "1", "Apple"),
      (1002L, "Geo-B", "1", "Durian")
    )
    assert(finalOutput.collect().toSet == expectedFinal)
  }

  test("tie-breaker: equal counts sorted alphabetically by item name") {
    // Build aggregated input directly: (geoId -> Map[item -> count])
    val aggregated = sc.parallelize(Seq(
      1001L -> Map("Banana" -> 2L, "Apple" -> 2L, "Cherry" -> 1L),
      1002L -> Map("Eggfruit" -> 1L, "Durian" -> 1L)
    ))

    // topX=2 → expect alphabetical order when counts tie
    val topItems = TopItemsWithSalting.topXPerGeo(aggregated, topX = 2).collect().toSet

    val expected = Set(
      // Apple(2) < Banana(2) alphabetically
      (1001L, "1", "Apple"),
      (1001L, "2", "Banana"),
      // Durian(1) < Eggfruit(1)
      (1002L, "1", "Durian"),
      (1002L, "2", "Eggfruit")
    )
    assert(topItems == expected)
  }
}
