package io.phdata.retirementage

import io.phdata.retirementage.domain.GlobalConfig
import io.phdata.retirementage.loadgen.LoadGeneratorConfig
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class KuduLoadGeneratorTest extends FunSuite {
  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .appName("load-generator")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.parquet.binaryAsString", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  val kuduContext =
    new KuduContext(GlobalConfig.kuduMasters.get.mkString(","), spark.sqlContext.sparkContext)

  ignore("Create test dataframe with specified payload and record size") {

  }
}
