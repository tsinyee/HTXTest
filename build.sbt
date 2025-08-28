ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

Test / parallelExecution := false

lazy val IT = config("it") extend Test

// JVM module flags so Spark can access internal NIO in forked test JVMs
lazy val sparkJvmMods = Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .configs(IT)
  .enablePlugins(ScalastylePlugin)
  .settings(
    inConfig(IT)(Defaults.testSettings),

    name := "HTXTest",
    idePackagePrefix := Some("com.htx.spark.test"),

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided",

      "org.scalatest" %% "scalatest" % "3.2.18" % Test,

      "org.apache.spark" %% "spark-core" % "3.5.1" % IT,
      "org.apache.spark" %% "spark-sql" % "3.5.1" % IT,
      "org.scalatest" %% "scalatest" % "3.2.18" % IT
    ),


    Test / fork := true,
    IT / fork := true,

    Test / javaOptions ++= sparkJvmMods,
    IT / javaOptions ++= sparkJvmMods,

    IT / test := (IT / test).dependsOn(Compile / packageBin).value
  )
