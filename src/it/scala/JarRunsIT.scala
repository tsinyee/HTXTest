package com.htx.spark.test

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.{Files, Paths}

class JarRunsIT extends AnyFunSuite {

  test("fat jar runs with spark-submit and produces expected parquet") {
    val workDir = Files.createTempDirectory("it-topitems-").toFile
    val inputADir = new File(workDir, "inputA")
    inputADir.mkdirs()
    val inputBDir = new File(workDir, "inputB")
    inputBDir.mkdirs()
    val outputDir = new File(workDir, "output")
    outputDir.mkdirs()

    val spark = SparkSession.builder().master("local[*]").appName("IT-DataGen").getOrCreate()
    import spark.implicits._

    Seq(
      (1001L, "Apple", 1L),
      (1001L, "Apple", 2L),
      (1001L, "Banana", 3L),
      (1001L, "Banana", 3L), // duplicate detectionId -> should dedupe
      (1002L, "Cherry", 4L),
      (1002L, "Durian", 5L),
      (1002L, "Eggfruit", 6L)
    ).toDF("geographical_location_oid", "item_name", "detection_oid")
      .write.mode(SaveMode.Overwrite).parquet(inputADir.getAbsolutePath)

    Seq(
      (1001L, "Geo-A"),
      (1002L, "Geo-B")
    ).toDF("geographical_location_oid", "geographical_location")
      .write.mode(SaveMode.Overwrite).parquet(inputBDir.getAbsolutePath)

    spark.stop()

    // find the packaged jar under target/scala-<bin>/
    val scalaBinary = sys.props.getOrElse("scala.binary.version", "2.12")
    val jarDir = Paths.get("target", s"scala-$scalaBinary").toFile
    val jar: File = Option(jarDir.listFiles()).getOrElse(Array.empty)
      .find(f => f.isFile && f.getName.endsWith(".jar") && f.getName.contains(s"_$scalaBinary"))
      .getOrElse {
        fail(s"No packaged jar found under ${jarDir.getAbsolutePath}. Did you run `sbt it:test` (which depends on package)?")
      }

    val mainClass = "com.htx.spark.test.TopItemsApp"
    val cmd = Seq(
      "spark-submit",
      "--master", "local[*]",
      "--class", mainClass,
      jar.getAbsolutePath,
      inputADir.getAbsolutePath,
      inputBDir.getAbsolutePath,
      outputDir.getAbsolutePath,
      "2"
    )
    val outBuf = new StringBuilder
    val errBuf = new StringBuilder
    val logger = scala.sys.process.ProcessLogger(
      (o: String) => outBuf.append(o).append('\n'),
      (e: String) => errBuf.append(e).append('\n')
    )

    val exit = scala.sys.process.Process(cmd, None).!(logger)
    assert(
      exit == 0,
      s"""
         |spark-submit failed (exit=$exit)
         |COMMAND:
         |${cmd.mkString(" ")}
         |
         |STDOUT:
         |${outBuf.toString}
         |
         |STDERR:
         |${errBuf.toString}
         |""".stripMargin
    )

    assert(exit == 0, s"spark-submit failed: ${cmd.mkString(" ")}")

    val spark2 = SparkSession.builder().master("local[*]").appName("IT-Validate").getOrCreate()
    val outDf = spark2.read.parquet(outputDir.getAbsolutePath)

    val rows = outDf
      .select("geographical_location_oid", "geographical_location", "item_rank", "item_name")
      .collect()
      .map(r => (r.getLong(0), r.getString(1), r.getString(2), r.getString(3)))
      .toSet

    val expected = Set(
      (1001L, "Geo-A", "1", "Apple"),
      (1001L, "Geo-A", "2", "Banana"),
      (1002L, "Geo-B", "1", "Cherry"),
      (1002L, "Geo-B", "2", "Durian")
    )
    assert(rows == expected)

    spark2.stop()
  }
}
