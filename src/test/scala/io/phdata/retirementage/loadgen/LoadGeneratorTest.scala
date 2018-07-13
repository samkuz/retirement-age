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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class LoadGeneratorTest extends FunSuite {
  val conf  = new SparkConf().setMaster("local[*]")
  val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

  test("Joining fact table to dimension table results in records > 0") {
    // Add a random seed for testing?
    // Create a fact-table
    val factCount = 10000
    val factDf    = LoadGenerator.generateTable(spark, factCount, 1).cache()

    // Create a dimension-table
    val dimensionCount = 5000
    val dimDf          = LoadGenerator.generateTableFromParent(spark, dimensionCount, 1, factCount, factDf)

    // Join fact-table with dimension-table over dimensionid in fact-table and the dimension-table id field
    val joinDf = factDf.join(dimDf, dimDf("id") === factDf("dimensionid"))

    assertResult(5000)(joinDf.count())
  }

  test("Joining dimension table to subdimension table results in records > 0") {
    // Create a fact-table
    val factCount = 10000
    val factDf    = LoadGenerator.generateTable(spark, factCount, 1).cache()

    // Create a dimension-table
    val dimensionCount = 5000
    val dimDf =
      LoadGenerator.generateTableFromParent(spark, dimensionCount, 1, factCount, factDf).cache()

    // Create a sub-dimension-table
    val subdimensionCount = 2000
    val subDf =
      LoadGenerator.generateTableFromParent(spark, subdimensionCount, 1, dimensionCount, dimDf)

    // Join fact-table with dimension-table over dimensionid in fact-table and the dimension-table id field
    val joinDf = dimDf.join(subDf, subDf("id") === dimDf("dimensionid"))

    assertResult(2000)(joinDf.count())
  }

  test("Create test dataframe with specified payload and record size") {
    // fact table count: 20,000
    val factCount = 20000
    // Create the test dataframe with 1 payload bytes
    val testDf = LoadGenerator.generateTable(spark, factCount, 1)

    // Ceiling function can make count off
    assertResult(20000)(testDf.count())
  }

  test("Create test dimensional dataframe where dimensional-count < fact-count") {
    // fact table count: 20,000
    // dimensional table count: 10,000

    // Create a fact-table
    val factCount = 20000
    val factDf    = LoadGenerator.generateTable(spark, factCount, 1)
    // Create a dimension-table
    val dimensionCount = 10000
    val dimDf          = LoadGenerator.generateTableFromParent(spark, dimensionCount, 1, factCount, factDf)

    assertResult(10000)(dimDf.count())
  }

  test("Create test dimensional dataframe where dimensional-count > fact-count") {
    // fact table count: 20,000
    // dimensional table count: 30,000

    // Create a fact-table
    val factCount = 20000
    val factDf    = LoadGenerator.generateTable(spark, factCount, 1)
    // Create a dimension-table
    val dimensionCount = 30000
    val dimDf          = LoadGenerator.generateTableFromParent(spark, dimensionCount, 1, factCount, factDf)

    assertResult(30000)(dimDf.count())
  }
}
