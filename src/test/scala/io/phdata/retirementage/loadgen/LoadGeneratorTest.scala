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
    // Create a fact-table
    val factCount = 5000
    val factDf    = LoadGenerator.generateTable(spark, factCount, 1)
    // Create a dimension-table
    val dimensionCount = 2000
    val dimDf          = LoadGenerator.generateTableFromParent(spark, dimensionCount, 1, factCount, factDf)

    // Join fact-table with dimension-table over dimensionId in fact-table and the dimension-table id field
    val joinDf = factDf.join(dimDf, factDf("dimensionId") === dimDf("id"))

    assertResult(2000)(joinDf.count())
  }

  ignore("Joining dimension table to subdimension table results in records > 0") {
    fail()
  }

  test("Create test dataframe with specified payload and record size") {
    // fact table count: 4,400
    val factCount = 4400
    // Create the test dataframe with 1 payload bytes
    val testDf = LoadGenerator.generateTable(spark, factCount, 1)

    // Ceiling function can make count off
    assertResult(4400)(testDf.count())
  }

  test("Create test dimensional dataframe where dimensional-count < fact-count") {
    // fact table count: 2,200
    // dimensional table count: 1,100

    // Create a fact-table
    val factCount = 2200
    val factDf    = LoadGenerator.generateTable(spark, factCount, 1)
    // Create a dimension-table
    val dimensionCount = 1100
    val dimDf          = LoadGenerator.generateTableFromParent(spark, dimensionCount, 1, factCount, factDf)

    assertResult(1100)(dimDf.count())
  }

  test("Create test dimensional dataframe where dimensional-count > fact-count") {
    // fact table count: 2,200
    // dimensional table count: 3,300

    // Create a fact-table
    val factCount = 2000
    val factDf    = LoadGenerator.generateTable(spark, factCount, 1)
    // Create a dimension-table
    val dimensionCount = 4000
    val dimDf          = LoadGenerator.generateTableFromParent(spark, dimensionCount, 1, factCount, factDf)

    assertResult(4000)(dimDf.count())
  }
}
