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
