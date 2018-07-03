/* Copyright 2018 phData Inc. */

package io.phdata.retirementage.loadgen

import java.util.UUID

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object LoadGenerator {
  def main(args: Array[String]): Unit = {

    val conf  = new LoadGeneratorConfig(args)
    val spark = new SparkSession()
    generateTables(spark, conf)
  }

  def generateTables(spark: SparkSession, conf: LoadGeneratorConfig): Unit = {
    // generate fact table
    val fdf = generateTable(spark, conf.factCount.getOrElse(0), 1)
    // write fact table to disk
    fdf.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${conf.databaseName}.${conf.factName.getOrElse("factLoadTest")}")
    // generate dimension table
    val ddf = generateTable(spark, conf.dimensionCount.getOrElse(0), 1)
    // write dimension table to disk
    ddf.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${conf.databaseName}.${conf.dimName.getOrElse("dimLoadTest")}")
    // generate subdimension table
    val sdf = generateTable(spark, conf.subDimensionCount.getOrElse(0), 1)
    // write subdimension table to disk
    sdf.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${conf.databaseName}.${conf.subName.getOrElse("subLoadTest")}")
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
      */
    val schema = StructType(StructField("id", StringType, false) :: Nil)
      .add(StructField("payload", StringType, false))
    var dataBuffer = ListBuffer[List[String]]()
    // Create a string with specified bytes
    var i          = 0
    var byteString = ""
    for (i <- 1 to payloadBytes) {
      byteString = byteString + "a"
    }
    // Create numRecords rows of data with a random UUID and attached a payload of specified bytes
    var j = 0
    for (j <- 1 to numRecords) {
      dataBuffer += List(UUID.randomUUID().toString.substring(0, 7), byteString)
    }
    val dataList = dataBuffer.toList
    // Creating rows and RDD for the DataFrame
    val rows = dataList.map(x => Row(x: _*))
    val rdd  = spark.sparkContext.makeRDD(rows)

    // Returning DataFrame
    spark.createDataFrame(rdd, schema)
  }

  /**
    * Generates a test dataframe with keys from the parent used as foreign keys
    * @param numRecords Number of records in the dataframe
    * @param payloadBytes Payload size
    * @return The test dataframe
    */
  def generateTableFromParent(numRecords: Int, payloadBytes: Int, parent: DataFrame): DataFrame =
    ???
}
