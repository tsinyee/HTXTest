# HTX xData Technical Test (Data Engineer)

## Overview

This project processes item data by geographical location using Apache Spark.  
It demonstrates:

- Deduplication of records
- Skew detection and salting
- Aggregation and top-X selection per geo id
- Joining with geo name metadata

Main application is in `src/main/scala`, tests in `src/test/scala` and `src/it/scala`.

- `TopItemsApp.scala`: Main Spark job
- `TopItemsWithSaltingApp.scala`: Spark job with salting for skew handling

All logic is tested with ScalaTest unit tests.

---

## Development Environment

- macOS 14.5 (Sonoma)
- JDK 11
- Scala 2.12.18
- sbt 1.11.5
- Apache Spark 3.5.1
- ScalaTest 3.2.18

---

## Setup

1. Clone the repository:
   ```bash
   git clone <your-repo-url>
   cd HTXTest
   ```
2. Build the project:
    ```bash
    sbt clean compile
     ```

3. Run all tests:

    ```bash
    sbt test
    ```
4. Run integration tests (these will package the fat JAR and execute it via spark-submit on your
   local Spark):
    ```bash
    sbt it:test
    ```
5. Run the main app:

    ```bash
    sbt "runMain com.htx.spark.TopItemsApp <inputA> <inputB> <output> <topX>"
    ```

