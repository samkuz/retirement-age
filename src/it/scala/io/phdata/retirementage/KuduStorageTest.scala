package io.phdata.retirementage

import java.util.UUID

import io.phdata.retirementage.domain.{Database, DatedTable}
import io.phdata.retirementage.filters.DatedTableFilter
import io.phdata.retirementage.storage.KuduStorage
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class KuduStorageTest extends FunSuite with SparkTestBase {
  val kuduMaster  = "localhost:7051"
  val kuduContext = new KuduContext(kuduMaster, spark.sqlContext.sparkContext)
  val schema      = StructType(StructField("date", LongType, false) :: Nil)

  test("creating a kudu table") {
    val kuduTableName = createKuduTable(schema, kuduContext)
    println("Created kuduTableName: " + kuduTableName)
    val table              = DatedTable(kuduTableName, "kudu", "date", 1, None, None, None)
    val database           = Database("default", Seq(table))
    val qualifiedTableName = s"${database.name}.${kuduTableName}"

    assert(kuduContext.tableExists(qualifiedTableName))
  }

  test("insert rows") {
    val kuduTableName = createKuduTable(schema, kuduContext)
    println("Created kuduTableName: " + kuduTableName)
    val table              = DatedTable(kuduTableName, "kudu", "date", 1, None, None, None)
    val database           = Database("default", Seq(table))
    val qualifiedTableName = s"${database.name}.${kuduTableName}"

    val data = TestObjects.smallDatasetSeconds
    val df   = createDataFrame(data, schema)

    kuduContext.insertRows(df, qualifiedTableName)

    val testDf =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> qualifiedTableName))
        .kudu

    assertResult(3)(testDf.count())
  }

  test("delete rows") {
    val kuduTableName = createKuduTable(schema, kuduContext)
    println("Delete rows test, kuduTableName: " + kuduTableName)
    val table              = DatedTable(kuduTableName, "kudu", "date", 1, None, None, None)
    val database           = Database("default", Seq(table))
    val qualifiedTableName = s"${database.name}.${kuduTableName}"

    val data = TestObjects.smallDatasetSeconds
    val df   = createDataFrame(data, schema)

    kuduContext.insertRows(df, qualifiedTableName)

    val filter = new DatedTableFilter(database, table) with KuduStorage

    val result = filter.expiredRecords()

    kuduContext.deleteRows(result, qualifiedTableName)

    val resultDf =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> qualifiedTableName))
        .kudu

    assertResult(1)(resultDf.count())
  }

  test("don't make changes on a dry run") {
    val kuduTableName = createKuduTable(schema, kuduContext)
    println("Created kuduTableName: " + kuduTableName)
    val table              = DatedTable(kuduTableName, "kudu", "date", 1, None, None, None)
    val database           = Database("default", Seq(table))
    val qualifiedTableName = s"${database.name}.${kuduTableName}"

    val data = TestObjects.smallDatasetSeconds
    val df   = createDataFrame(data, schema)

    kuduContext.insertRows(df, qualifiedTableName)

    val filter = new DatedTableFilter(database, table) with KuduStorage

    filter.doFilter(computeCountsFlag = false, dryRun = true)

    val resultDf =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> qualifiedTableName))
        .kudu

    assertResult(3)(resultDf.count())
  }

  ignore("delete dimensional table records"){
    val kuduTableName = createKuduTable(schema, kuduContext)
    val kuduDimTableName = createKuduTable(schema, kuduContext)
    // Creating fact kudu table
    val table              = DatedTable(kuduTableName, "kudu", "date", 1, None, None, None)
    val database           = Database("default", Seq(table))
    val qualifiedTableName = s"${database.name}.${kuduTableName}"
    val data = TestObjects.smallDatasetSeconds
    val df   = createDataFrame(data, schema)
    kuduContext.insertRows(df, qualifiedTableName)
    //Creating dim kudu table
    val dimTable = DatedTable(kuduTableName, "kudu", "date", 1, None, None, None)
    val dimDatabase = Database("default", Seq(table))
    val dimQualifiedTableName = s"${database.name}.${kuduTableName}"

  }

  def createKuduTable(schema: StructType, kc: KuduContext): String = {
    val kuduTableName = UUID.randomUUID().toString().substring(0, 5)

    val qualifiedtableName = s"default.${kuduTableName}"

    if (kc.tableExists(qualifiedtableName)) {
      kc.deleteTable(qualifiedtableName)
    }

    val kuduTableSchema = StructType(StructField("date", LongType, false) :: Nil)

    val kuduPrimaryKey = Seq("date")

    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.setRangePartitionColumns(List("date").asJava).setNumReplicas(1)

    kc.createTable(
                   // Table name, schema, primary key and options
                   qualifiedtableName,
                   kuduTableSchema,
                   kuduPrimaryKey,
                   kuduTableOptions)

    kuduTableName
  }
}
