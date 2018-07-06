/* Copyright 2018 phData Inc. */

package io.phdata.retirementage.loadgen

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class LoadGeneratorTest extends FunSuite {
  val conf  = new SparkConf().setMaster("local[*]")
  val spark = SparkSession.builder().enableHiveSupport().config(conf)

  ignore("Joining fact table to dimension table results in records > 0") {
    fail()
  }

  ignore("Joining dimension table to subdimension table results in records > 0") {
    fail()
  }

  test("Create test dataframe with specified payload and record size") {
    val sparkConf = new SparkConf()
    val spark = SparkSession
      .builder()
      .appName("retirement-age")
      .master("local[3]")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val testDf = LoadGenerator.generateTable(spark, 123456, 1)

    // Ceiling function can make count off by 1
    // Right now my initDF that I fold onto adds 1 to the numRecords
    assertResult(123457)(testDf.count())
  }

  ignore(
    "Create test dataframe with specified payload, record size, and foreign keys to parent table") {
    fail()
  }
}
