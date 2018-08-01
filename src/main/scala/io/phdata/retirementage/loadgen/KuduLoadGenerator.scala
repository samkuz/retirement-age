package io.phdata.retirementage.loadgen

import io.phdata.retirementage.domain.GlobalConfig
import io.phdata.retirementage.loadgen.LoadGenerator.{generateTable, generateTableFromParent}
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.kudu.spark.kudu._
import scala.collection.JavaConverters._

object KuduLoadGenerator {
  val defaultSchema = StructType(StructField("id", StringType, false) :: Nil)
    .add(StructField("payload", StringType, false))
    .add(StructField("dimension_id", StringType, false))
    .add(StructField("expiration_date", StringType, false))
  val defaultKey = Seq("id")

  def main(args: Array[String]): Unit = {

    val conf = new LoadGeneratorConfig(args)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("load-generator")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.parquet.binaryAsString", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    GlobalConfig.kuduMasters = Some(List("master3.valhalla.phdata.io:7051"))

    val kuduContext =
      new KuduContext(GlobalConfig.kuduMasters.get.mkString(","), spark.sqlContext.sparkContext)
    if (conf.testKudu()) {
      readKuduTable(spark, kuduContext, conf)
    } else {
      generateTables(spark, kuduContext, conf)
    }
  }

  def generateTables(spark: SparkSession,
                     kuduContext: KuduContext,
                     conf: LoadGeneratorConfig): Unit = {
    val factDf =
      generateTable(spark, conf.factCount(), 1).persist(StorageLevel.MEMORY_AND_DISK)

    createKuduTable(kuduContext, conf.factName(), 10)

    kuduContext.insertRows(factDf, conf.factName())

    val dimensionDf =
      generateTableFromParent(spark, conf.dimensionCount(), 1, conf.factCount(), factDf)
        .persist(StorageLevel.MEMORY_AND_DISK)

    createKuduTable(kuduContext, conf.dimName(), 10)

    kuduContext.insertRows(dimensionDf, conf.dimName())

    val subdimmensionDf =
      generateTableFromParent(spark,
                              conf.subDimensionCount(),
                              1,
                              conf.dimensionCount(),
                              dimensionDf)
        .persist(StorageLevel.MEMORY_AND_DISK)

    createKuduTable(kuduContext, conf.subName(), 10)

    kuduContext.insertRows(subdimmensionDf, conf.subName())
  }

  /*
   * createKuduTable creates a kudu table given:
   * tableName: The name of the kudu table to create
   * numBuckets: The number of buckets for the hash partition
   */
  def createKuduTable(kuduContext: KuduContext, tableName: String, numBuckets: Int): Unit = {
    val tableOptions = new CreateTableOptions()
      .setNumReplicas(1)
      .addHashPartitions(List(defaultKey(0)).asJava, numBuckets)

    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }
    kuduContext.createTable(tableName, defaultSchema, defaultKey, tableOptions)
  }

  /*
   * readKuduTable reads a kudu table.
   */
  def readKuduTable(spark: SparkSession,
                    kuduContext: KuduContext,
                    conf: LoadGeneratorConfig): Unit = {
    if (kuduContext.tableExists(conf.factName())) {
      val tempDf = spark.sqlContext.read
        .options(
          Map("kudu.master" -> GlobalConfig.kuduMasters.get.mkString(","),
              "kudu.table"  -> conf.factName()))
        .kudu
      println(s"The count of ${conf.factName()} is: " + tempDf.count())
    }
    if (kuduContext.tableExists(conf.dimName())) {
      val tempDf2 = spark.sqlContext.read
        .options(
          Map("kudu.master" -> GlobalConfig.kuduMasters.get.mkString(","),
              "kudu.table"  -> conf.dimName()))
        .kudu
      println(s"The count of ${conf.dimName()} is: " + tempDf2.count())
    }
    if (kuduContext.tableExists(conf.subName())) {
      val tempDf3 = spark.sqlContext.read
        .options(
          Map("kudu.master" -> GlobalConfig.kuduMasters.get.mkString(","),
              "kudu.table"  -> conf.subName()))
        .kudu
      println(s"The count of ${conf.subName()} is: " + tempDf3.count())
    }
  }

}
