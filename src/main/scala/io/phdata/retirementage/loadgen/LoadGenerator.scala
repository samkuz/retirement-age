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
    // write fact table to disk
    // generate dimension table
    // write dimension table to disk
    // generate subdimension table
    // write subdimension table to disk
  }

  /**
    * Generates a test dataframe
    * @param numRecords Number of records in the dataframe
    * @param payloadBytes Payload size
    * @return The test dataframe
    */
  def generateTable(spark: SparkSession, numRecords: Int, payloadBytes: Int): DataFrame = {
    val schema = StructType(StructField("id", StringType, false) :: Nil)
      .add(StructField("payload", StringType, false))
    var dataBuffer = ListBuffer[List[String]]()
    var i          = 0
    var byteString = ""
    for (i <- 1 to payloadBytes) {
      byteString = byteString + "a"
    }
    var j = 0
    for (j <- 1 to numRecords) {
      dataBuffer += List(UUID.randomUUID().toString.substring(0, 7), byteString)
    }
    val dataList = dataBuffer.toList

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
