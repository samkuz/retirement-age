package io.phdata.retirementage

import java.sql.Date
import java.util.UUID

import io.phdata.retirementage.domain._
import io.phdata.retirementage.filters.DatedTableFilter
import io.phdata.retirementage.storage.KuduStorage
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class KuduStorageTest extends FunSuite with SparkTestBase {
  val kuduMaster    = "localhost:7051"
  val kuduContext   = new KuduContext(kuduMaster, spark.sqlContext.sparkContext)
  val defaultSchema = StructType(StructField("date", LongType, false) :: Nil)
  val defaultKey    = Seq("date")

  test("creating a kudu table") {
    val kuduTableName = createKuduTable(defaultSchema, defaultKey, kuduContext)
    println("Created kuduTableName: " + kuduTableName)
    val table              = DatedTable(kuduTableName, "kudu", "date", 1, None, None, None)
    val database           = Database("default", Seq(table))
    val qualifiedTableName = s"${database.name}.${kuduTableName}"

    assert(kuduContext.tableExists(qualifiedTableName))
  }

  test("insert rows") {
    val kuduTableName = createKuduTable(defaultSchema, defaultKey, kuduContext)
    println("Created kuduTableName: " + kuduTableName)
    val table              = DatedTable(kuduTableName, "kudu", "date", 1, None, None, None)
    val database           = Database("default", Seq(table))
    val qualifiedTableName = s"${database.name}.${kuduTableName}"

    val data = TestObjects.smallDatasetSeconds
    val df   = createDataFrame(data, defaultSchema)

    kuduContext.insertRows(df, qualifiedTableName)

    val testDf =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> qualifiedTableName))
        .kudu

    assertResult(3)(testDf.count())
  }

  test("delete rows") {
    val kuduTableName = createKuduTable(defaultSchema, defaultKey, kuduContext)
    println("Delete rows test, kuduTableName: " + kuduTableName)
    val table              = DatedTable(kuduTableName, "kudu", "date", 1, None, None, None)
    val database           = Database("default", Seq(table))
    val qualifiedTableName = s"${database.name}.${kuduTableName}"

    val data = TestObjects.smallDatasetSeconds
    val df   = createDataFrame(data, defaultSchema)

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
    val kuduTableName = createKuduTable(defaultSchema, defaultKey, kuduContext)
    println("Created kuduTableName: " + kuduTableName)
    val table              = DatedTable(kuduTableName, "kudu", "date", 1, None, None, None)
    val database           = Database("default", Seq(table))
    val qualifiedTableName = s"${database.name}.${kuduTableName}"

    val data = TestObjects.smallDatasetSeconds
    val df   = createDataFrame(data, defaultSchema)

    kuduContext.insertRows(df, qualifiedTableName)

    val filter = new DatedTableFilter(database, table) with KuduStorage

    filter.doFilter(computeCountsFlag = false, dryRun = true)

    val resultDf =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> qualifiedTableName))
        .kudu

    assertResult(3)(resultDf.count())
  }

  test("delete dimensional table records") {
    // Create fact table
    val kuduTableName = createKuduTable(defaultSchema, defaultKey, kuduContext)
    // Create dimensional table
    val dimensionSchema = StructType(
      StructField("id", StringType, false) :: StructField("date", LongType, false) :: Nil)
    val dimensionKey = Seq("id")
    val kuduDimTableName = createKuduTable(dimensionSchema, dimensionKey, kuduContext)

    val join = JoinOn("date", "date")
    val dimensionTable: ChildTable = ChildTable(kuduDimTableName, "kudu", join, None, None)
    val table = DatedTable(kuduTableName, "kudu", "date", 1, None, None, Some(List(dimensionTable)))
    val database = Database("default", Seq(table))

    val factData = TestObjects.smallDatasetSeconds
    val factDf = createDataFrame(factData, defaultSchema)

    val dimData = List(List("1", Date.valueOf("2018-12-25").getTime / 1000),
      List("2", Date.valueOf("2017-12-25").getTime / 1000),
      List("3", Date.valueOf("2016-12-25").getTime / 1000))
    val dimDf = createDataFrame(dimData, dimensionSchema)

    val qualifiedTableName = s"${database.name}.${kuduTableName}"
    val dimQualifiedTableName = s"${database.name}.${kuduDimTableName}"
    // Inserting rows into fact table
    kuduContext.insertRows(factDf, qualifiedTableName)
    // Inserting rows into dimension table
    kuduContext.insertRows(dimDf, dimQualifiedTableName)

    val filter = new DatedTableFilter(database, table) with KuduStorage
    filter.doFilter(computeCountsFlag = false)
    // Read dimensional table and verify filtering
    val resultDf =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> dimQualifiedTableName))
        .kudu

    assertResult(1)(resultDf.count())
  }

  test("delete subdimensional table records"){
    // Create fact table
    val kuduTableName = createKuduTable(defaultSchema, defaultKey, kuduContext)
    // Create dimensional table
    val dimensionSchema = StructType(
      StructField("id", StringType, false) :: StructField("date", LongType, false) :: Nil)
    val dimensionKey = Seq("id")
    val kuduDimTableName = createKuduTable(dimensionSchema, dimensionKey, kuduContext)
    // Create a subdimensional table
    val subdimSchema = StructType(
      StructField("sub_id", StringType, false) :: StructField("id", StringType, false) :: StructField("date", LongType, false) :: Nil)
    val subdimKey = Seq("sub_id")
    val kuduSubTableName = createKuduTable(subdimSchema, subdimKey, kuduContext)

    val factdimjoin = JoinOn("date", "date")
    val dimsubjoin = JoinOn("id", "id")
    val subdimTable: ChildTable = ChildTable(kuduSubTableName, "kudu", dimsubjoin, None, None)
    val dimensionTable: ChildTable = ChildTable(kuduDimTableName, "kudu", factdimjoin, None, Some(List(subdimTable)))
    val table = DatedTable(kuduTableName, "kudu", "date", 1, None, None, Some(List(dimensionTable)))
    val database = Database("default", Seq(table))

    val factData = TestObjects.smallDatasetSeconds
    val factDf = createDataFrame(factData, defaultSchema)

    val dimData = List(List("1", Date.valueOf("2018-12-25").getTime / 1000),
      List("2", Date.valueOf("2017-12-25").getTime / 1000),
      List("3", Date.valueOf("2016-12-25").getTime / 1000))
    val dimDf = createDataFrame(dimData, dimensionSchema)

    val subData = List(List("10", "1", Date.valueOf("2018-12-25").getTime / 1000),
      List("20", "2", Date.valueOf("2017-12-25").getTime / 1000),
      List("30", "3", Date.valueOf("2016-12-25").getTime / 1000))
    val subDf = createDataFrame(subData, subdimSchema)

    val qualifiedTableName = s"${database.name}.${kuduTableName}"
    val dimQualifiedTableName = s"${database.name}.${kuduDimTableName}"
    val subQualifiedTableName = s"${database.name}.${kuduSubTableName}"
    // Inserting rows into fact table
    kuduContext.insertRows(factDf, qualifiedTableName)
    // Inserting rows into dimension table
    kuduContext.insertRows(dimDf, dimQualifiedTableName)
    // Inserting rows into sub dimension table
    kuduContext.insertRows(subDf, subQualifiedTableName)

    val filter = new DatedTableFilter(database, table) with KuduStorage
    filter.doFilter(computeCountsFlag = false)
    // Read dimensional table and verify filtering
    val resultDf =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> subQualifiedTableName))
        .kudu

    assertResult(1)(resultDf.count())
  }

  def createKuduTable(schema: StructType, primaryKey: Seq[String], kc: KuduContext): String = {
    val kuduTableName = UUID.randomUUID().toString().substring(0, 5)

    val qualifiedtableName = s"default.${kuduTableName}"

    if (kc.tableExists(qualifiedtableName)) {
      kc.deleteTable(qualifiedtableName)
    }
    println("Primary key is: " + primaryKey(0))
    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.setRangePartitionColumns(List(primaryKey(0)).asJava).setNumReplicas(1)

    kc.createTable(
                   // Table name, schema, primary key and options
                   qualifiedtableName,
                   schema,
                   primaryKey,
                   kuduTableOptions)

    kuduTableName
  }
}
