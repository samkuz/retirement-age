/* Copyright 2018 phData Inc. */

package io.phdata.retirementage.loadgen

import java.util.UUID
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.lit

object LoadGenerator {
  def main(args: Array[String]): Unit = {

    val conf  = new LoadGeneratorConfig(args)
    val spark = new SparkSession()
    generateTables(spark, conf)
  }

  def generateTables(spark: SparkSession, conf: LoadGeneratorConfig): Unit = {
    // generate fact table
    val factdf = generateTable(spark, conf.factCount.getOrElse(0), 1)
    // write fact table to disk
    factdf.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${conf.databaseName}.${conf.factName.getOrElse("factLoadTest")}")
    // generate dimension table
    val dimdf = generateTableFromParent(spark, conf.dimensionCount.getOrElse(0), 3, factdf)
    // write dimension table to disk
    dimdf.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${conf.databaseName}.${conf.dimName.getOrElse("dimLoadTest")}")
    // generate subdimension table
    val sdf = generateTableFromParent(spark, conf.subDimensionCount.getOrElse(0), 1, dimdf)
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
      *   -- date: String (nullabe = false)
      */
    val schema = StructType(StructField("id", StringType, false) :: Nil)
      .add(StructField("payload", StringType, false))
      .add(StructField("date", StringType, false))
    // Create a string with specified bytes
    val byteString = "a" * payloadBytes

    var dataBuffer = ListBuffer[List[String]]()

    // Start: new create from numRecords
    val numUnions    = math.ceil(numRecords / 5000).toInt
    val unionRecords = math.ceil(numRecords / numUnions).toInt

    // Create initial dataframe to Union following dataframes to
    val initList = List(List(UUID.randomUUID().toString.substring(0, 9), byteString, tempDateGenerator()))

    val rows = initList.map(x => Row(x: _*))
    val rdd  = spark.sparkContext.makeRDD(rows)

    val fullDF = spark.createDataFrame(rdd,schema)
    // Create lists of size 5000 and then convert to dataframes to union to aggregate dataframe
    for (x <- 1 to numUnions) {
        dataBuffer = null
        for (y <- 1 to unionRecords) {
          dataBuffer += List(UUID.randomUUID().toString.substring(0, 9),
                             byteString,
                             tempDateGenerator())
        }

        val dataList = dataBuffer.toList
        // Creating rows and RDD for the DataFrame
        val temprows = dataList.map(x => Row(x: _*))
        val temprdd  = spark.sparkContext.makeRDD(temprows)

        val tempdf = spark.createDataFrame(temprdd, schema)

        fullDF.union(tempdf)
      }

    fullDF

    // END

    /*
    // Create numRecords rows of data with a random UUID and attached a payload of specified bytes and a date
    for (j <- 1 to numRecords) {
      dataBuffer += List(UUID.randomUUID().toString.substring(0, 9),
                         byteString,
                         tempDateGenerator(j))
    }
    val dataList = dataBuffer.toList
    // Creating rows and RDD for the DataFrame
    val rows = dataList.map(x => Row(x: _*))
    val rdd  = spark.sparkContext.makeRDD(rows)

    // Returning DataFrame
    spark.createDataFrame(rdd, schema)
   */
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
                              parent: DataFrame): DataFrame = {
    val byteString = "a" * payloadBytes

    // Can the numRecords for the child be larger than the parent?
    // Should the foreign key be the parent's UUID created earlier?
    parent.select("id").limit(numRecords).withColumn("payload", lit(byteString))

  }
}
