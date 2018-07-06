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

  ignore("Joining fact table to dimension table results in records > 0") {
    fail()
  }

  ignore("Joining dimension table to subdimension table results in records > 0") {
    fail()
  }

  test("Create test dataframe with specified payload and record size") {
    // Initializing the args to pass to LoadGeneratorConfig
    // fact-count is the important argument here
    val factCount = 1234564


    val testDf = LoadGenerator.generateTable(spark, factCount, 1)

    // Ceiling function can make count off by 1
    assertResult(1234564)(testDf.count())
  }

  test("Create test dimensional dataframe where dimensional-count < fact-count") {
    // fact table count: 1,200,000
    // dimensional table count: 750,000

    val factCount = 1200000
    val factDf    = LoadGenerator.generateTable(spark, factCount, 1)

    val dimensionCount = 750000
    val dimDf = LoadGenerator.generateTableFromParent(spark, dimensionCount, 1, factCount, factDf)

    assertResult(750000)(dimDf.count())
  }

  test("Create test dimensional dataframe where dimensional-count > fact-count") {
    // fact table count: 1,200,000
    // dimensional table count: 750,000
    
    // Create a fact-table
    val factCount = 750000
    val factDf    = LoadGenerator.generateTable(spark, factCount, 1)
    // Create a dimension-table
    val dimensionCount = 1200000
    val dimDf = LoadGenerator.generateTableFromParent(spark, dimensionCount, 1, factCount, factDf)

    assertResult(1200000)(dimDf.count())
  }
}
