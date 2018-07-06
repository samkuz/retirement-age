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

    val conf      = new LoadGeneratorConfig(args)
    val sparkConf = new SparkConf()

    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    generateTables(spark, conf)
  }

  def generateTables(spark: SparkSession, conf: LoadGeneratorConfig): Unit = {
    // generate fact table
    val factdf = generateTable(spark, conf.factCount.apply(), 1)
    // write fact table to disk
    factdf.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${conf.databaseName}.${conf.factName.apply()}")
    // generate dimension table
    val dimensionDf =
      generateTableFromParent(spark, conf.dimensionCount.apply(), 3, conf.factCount.apply(), factdf)
    // write dimension table to disk
    dimensionDf.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${conf.databaseName}.${conf.dimName.apply()}")
    // generate subdimension table
    val subDimensionDf = generateTableFromParent(spark,
                                                 conf.subDimensionCount.apply(),
                                                 1,
                                                 conf.dimensionCount.apply(),
                                                 dimensionDf)
    // write subdimension table to disk
    subDimensionDf.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${conf.databaseName}.${conf.subName.apply()}")
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
      *   -- date: String (nullabe = false)
      */
    val schema = StructType(StructField("id", StringType, false) :: Nil)
      .add(StructField("payload", StringType, false))
      .add(StructField("dimensionId", StringType, false))
      .add(StructField("date", StringType, false))
    // Create a string with specified bytes
    val byteString = "a" * payloadBytes

    // Num of records for each partition
    val recordNum        = 1000000
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

    // Create a dimension dataframe
    val dimensionDf = parent
      .select("dimensionId")
      .limit(numRecords)
      .withColumn("payload", lit(byteString))
      .withColumn("subdimensionId", lit(UUID.randomUUID().toString()))
      .withColumn("date", lit("2222-22-22"))

    // If numRecords > parentNumRecords creates the difference to union to the dimension dataframe
    if (numRecords > parentNumRecords) {
      val newRowsnum = numRecords - parentNumRecords
      val tempDf     = generateTable(spark, newRowsnum, payloadBytes)

      dimensionDf.union(tempDf)
    } else {
      dimensionDf
    }

  }
}
