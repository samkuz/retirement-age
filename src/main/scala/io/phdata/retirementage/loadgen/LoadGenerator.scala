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
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

object LoadGenerator {
  def main(args: Array[String]): Unit = {
    val conf = new LoadGeneratorConfig(args)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("load-generator")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.parquet.binaryAsString", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    /**
      * Initialize spark job by
      * spark2-submit --deploy-mode client --master yarn
      * --class io.phdata.retirementage.loadgen.LoadGenerator
      * <path to jar> --fact-count <#> --dimension-count <#> --subdimension-count <#> --database-name <String>
      *   (optional) --fact-name <name> --dim-name <name> --subdim-name <name>
      */

    generateTables(spark, conf)
  }

  def generateTables(spark: SparkSession, conf: LoadGeneratorConfig): Unit = {
    // generate fact table
    val factdf = generateTable(spark, conf.factCount(), 1)
    // write fact table to disk
    factdf.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"${conf.databaseName()}.${conf.factName()}")
    // generate dimension table
    val dimensionDf =
      generateTableFromParent(spark, conf.dimensionCount(), 3, conf.factCount(), factdf)
    // write dimension table to disk
    dimensionDf.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"${conf.databaseName()}.${conf.dimName()}")
    // generate subdimension table
    val subDimensionDf = generateTableFromParent(spark,
                                                 conf.subDimensionCount(),
                                                 1,
                                                 conf.dimensionCount(),
                                                 dimensionDf)
    // write subdimension table to disk
    subDimensionDf.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"${conf.databaseName()}.${conf.subName()}")
  }

  /**
    * Generates a test dataframe
    * @param numRecords Number of records in the dataframe
    * @param payloadBytes Payload size
    * @return The test dataframe
    */
  def generateTable(spark: SparkSession, numRecords: Int, payloadBytes: Int): DataFrame = {

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

    // Num of records for each partition
    val recordNum        = 10000
    val numRecordsDouble = numRecords.toDouble

    // Calculate the number of unions to be made, and the records in each partition
    val numPartitions    = math.ceil(numRecordsDouble / recordNum).toInt
    val partitionRecords = math.ceil(numRecordsDouble / numPartitions).toInt

    // Create a sequence of dataframes
    val alldata = (1 to numPartitions).map(x => {
      val rawdata = (1 to partitionRecords)
      val newdata =
        rawdata.map(
          x =>
            Seq(UUID.randomUUID().toString(),
                byteString,
                UUID.randomUUID().toString(),
                tempDateGenerator()))
      val rows = newdata.map(x => Row(x: _*))
      val rdd  = spark.sparkContext.makeRDD(rows)
      spark.createDataFrame(rdd, schema)
    })

    // Union the sequence of dataframes onto initDF
    alldata.reduce((l, r) => l.union(r))
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
