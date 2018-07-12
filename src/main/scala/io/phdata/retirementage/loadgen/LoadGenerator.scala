/*
 * Copyright 2018 phData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.phdata.retirementage.loadgen

import java.util.UUID
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit

object LoadGenerator {
  def main(args: Array[String]): Unit = {

    /**
      * Initialize spark job by
      * spark2-submit --deploy-mode client --master yarn
      * --class io.phdata.retirementage.loadgen.LoadGenerator
      * <path to jar> --fact-count <#> --dimension-count <#> --subdimension-count <#> --database-name <String>
      *   (optional) --fact-name <name> --dim-name <name> --subdim-name <name>
      */
    val conf = new LoadGeneratorConfig(args)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("load-generator")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.parquet.binaryAsString", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    generateTables(spark, conf)
  }

  def generateTables(spark: SparkSession, conf: LoadGeneratorConfig): Unit = {
    // test stuff
    val coalNum = conf.factCount() / 250000

    // generate fact table
    val factdf = generateTable(spark, conf.factCount(), 1)
    // write fact table to disk
    factdf.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"${conf.databaseName()}.${conf.factName()}")

    // generate dimension table
//    val dimensionDf =
//      generateTableFromParent(spark, conf.dimensionCount(), 3, conf.factCount(), factdf)
//    // write dimension table to disk
//    dimensionDf.write
//      .mode(SaveMode.Overwrite)
//      .saveAsTable(s"${conf.databaseName()}.${conf.dimName()}")
//    // generate subdimension table
//    val subDimensionDf = generateTableFromParent(spark,
//                                                 conf.subDimensionCount(),
//                                                 1,
//                                                 conf.dimensionCount(),
//                                                 dimensionDf)
//    // write subdimension table to disk
//    subDimensionDf.write
//      .mode(SaveMode.Overwrite)
//      .saveAsTable(s"${conf.databaseName()}.${conf.subName()}")
  }

  // Creating a case class to use for generateTable
  case class LoadGenTemp(id: String, payload: String, dimensionId: String, expirationdate: String)

  /**
    * Generates a test dataframe
    * @param numRecords Number of records in the dataframe
    * @param payloadBytes Payload size
    * @return The test dataframe
    */
  def generateTable(spark: SparkSession, numRecords: Int, payloadBytes: Int) = {

    /**
      * Create the following schema:
      *   -- id: String (nullable = false)
      *   -- payload: String (nullable = false)
      *   -- dimensionId: String (nullable = false)
      *   -- expirationDate: String (nullabe = false)
      */
    val schema = StructType(StructField("id", StringType, false) :: Nil)
      .add(StructField("payload", StringType, false))
      .add(StructField("dimensionId", StringType, false))
      .add(StructField("expirationDate", StringType, false))
    // Create a string with specified bytes
    val byteString = "a" * payloadBytes

    // Maximum number of records per partition
    val maxPartitionRecords = 50000

    // Calculating the number of partitions and the records per partition
    val numPartitions       = math.ceil(numRecords.toDouble / maxPartitionRecords).toInt
    val recordsPerPartition = math.ceil(numRecords.toDouble / numPartitions).toInt

    val newdata = Range(1, recordsPerPartition).map(
      x =>
        Seq(UUID.randomUUID().toString(),
            byteString,
            UUID.randomUUID().toString(),
            tempDateGenerator()))
    val rows = newdata.map(x => Row(x: _*))
    val rdd  = spark.sparkContext.makeRDD(rows)
    val df   = spark.createDataFrame(rdd, schema)

    val dfs = Range(1, numPartitions).map(x => df)

    // Union the Sequence of DataFrames onto finalTable
    val duplicateDf: DataFrame = dfs.reduce((l, r) => l.union(r))

    import spark.implicits._
    // Create temporary dataset to make changes on
    val tempDs: Dataset[LoadGenTemp] = duplicateDf.as[LoadGenTemp]

    // Create random data
    val finalDf: Dataset[LoadGenTemp] = tempDs.map {
      case change if true =>
        change.copy(id = UUID.randomUUID().toString)
        change.copy(dimensionId = UUID.randomUUID().toString)
        change.copy(expirationdate = tempDateGenerator())
    }

    //Convert back to a DF and return
    // Return
    finalDf.toDF()
  }

  // Create date data with 2016-12-25, 2017-12-25, 2018-12-25
  def tempDateGenerator(): String = {
    val r = new scala.util.Random()
    val c = 0 + r.nextInt(3)
    c match {
      case 0 => "2016-12-25"
      case 1 => "2017-12-25"
      case 2 => "2018-12-25"
      case _ => "2222-22-22"
    }
  }

  /**
    * Generates a test dataframe with keys from the parent used as foreign keys
    * @param numRecords Number of records in the dataframe
    * @param payloadBytes Payload size
    * @return The test dataframe
    */
  def generateTableFromParent(spark: SparkSession,
                              numRecords: Int,
                              payloadBytes: Int,
                              parentNumRecords: Int,
                              parent: DataFrame): DataFrame = {

    val byteString = "a" * payloadBytes

    // Create a temporary dimension dataframe with the incorrect column names
    val oldDimensionDf = parent
      .select("dimensionId")
      .limit(numRecords)
      .withColumn("payload", lit(byteString))
      .withColumn("subdimensionId", lit(UUID.randomUUID().toString()))
      .withColumn("expirationDate", lit("2222-22-22"))
    // Correct column names
    val newNames = Seq("id", "payload", "dimensionId", "expirationDate")
    // Create a dimension DF with correct column names
    val dimensionDf = oldDimensionDf.toDF(newNames: _*)

    // If numRecords > parentNumRecords creates the difference to union to the dimension dataframe
    if (numRecords > parentNumRecords) {
      val newRowsNum = numRecords - parentNumRecords
      val tempDf     = generateTable(spark, newRowsNum, payloadBytes)

      dimensionDf.union(tempDf)
    } else {
      dimensionDf
    }

  }
}
