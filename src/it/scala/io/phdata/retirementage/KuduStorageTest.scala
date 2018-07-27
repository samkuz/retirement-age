package io.phdata.retirementage

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
  GlobalConfig.kuduMasters = Some(List("localhost:7051"))
  val kuduMaster  = GlobalConfig.kuduMasters.get.mkString(",")
  val kuduContext = new KuduContext(kuduMaster, spark.sqlContext.sparkContext)

  test("don't make changes on a dry run") {
    // Randomly generate table name
    val kuduTableName =
      createKuduTable(KuduObjects.defaultFactSchema, KuduObjects.defaultFactKey, kuduContext)
    // Create table and database objects
    val factTable =
      DatedTable(kuduTableName, "kudu", "date", 1, None, None, None)
    val database = Database("impala::default", Seq(factTable))

    val qualifiedTableName = createQualifiedTableName(database, factTable)
    // Insert test data
    insertKuduData(factTable, TestObjects.smallDatasetSeconds, KuduObjects.defaultFactSchema)

    val filter = new DatedTableFilter(database, factTable) with KuduStorage

    filter.doFilter(computeCountsFlag = false, dryRun = true)

    val resultDf =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> qualifiedTableName))
        .kudu

    assertResult(3)(resultDf.count())
  }

  test("delete expired fact table records") {
    val kuduTableName =
      createKuduTable(KuduObjects.defaultFactSchema, KuduObjects.defaultFactKey, kuduContext)

    val factTable =
      DatedTable(kuduTableName, "kudu", "date", 1, None, None, None)
    val database = Database("impala::default", Seq(factTable))

    val qualifiedTableName = createQualifiedTableName(database, factTable)

    insertKuduData(factTable, TestObjects.smallDatasetSeconds, KuduObjects.defaultFactSchema)

    val filter = new DatedTableFilter(database, factTable) with KuduStorage

    val result = filter.doFilter(computeCountsFlag = true, dryRun = false)

    val resultDf =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> qualifiedTableName))
        .kudu

    assertResult(1)(resultDf.count())
  }

  test("delete expired dimensional table records") {
    val factTableName =
      createKuduTable(KuduObjects.defaultFactSchema, KuduObjects.defaultFactKey, kuduContext)
    val dimTableName =
      createKuduTable(KuduObjects.defaultDimSchema, KuduObjects.defaultDimKey, kuduContext)
    val dimensionTable: ChildTable =
      ChildTable(dimTableName, "kudu", KuduObjects.defaultFactJoin, None, None)
    val factTable =
      DatedTable(factTableName, "kudu", "date", 1, None, None, Some(List(dimensionTable)))
    val database = Database("impala::default", Seq(factTable))

    insertKuduData(factTable, TestObjects.smallDatasetSeconds, KuduObjects.defaultFactSchema)

    insertKuduData(dimensionTable, KuduObjects.defaultDimData, KuduObjects.defaultDimSchema)

    val dimQualifiedTableName = s"${database.name}.${dimTableName}"

    val filter = new DatedTableFilter(database, factTable) with KuduStorage
    filter.doFilter(computeCountsFlag = false)

    val resultDf =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> dimQualifiedTableName))
        .kudu

    assertResult(1)(resultDf.count())
  }

  test("delete expired subdimensional table records") {
    val factTableName =
      createKuduTable(KuduObjects.defaultFactSchema, KuduObjects.defaultFactKey, kuduContext)
    val dimTableName =
      createKuduTable(KuduObjects.defaultDimSchema, KuduObjects.defaultDimKey, kuduContext)
    val subTableName =
      createKuduTable(KuduObjects.defaultSubSchema, KuduObjects.defaultSubKey, kuduContext)

    val subdimTable: ChildTable =
      ChildTable(subTableName, "kudu", KuduObjects.defaultDimJoin, None, None)
    val dimensionTable: ChildTable =
      ChildTable(dimTableName, "kudu", KuduObjects.defaultFactJoin, None, Some(List(subdimTable)))
    val factTable =
      DatedTable(factTableName, "kudu", "date", 1, None, None, Some(List(dimensionTable)))
    val database = Database("impala::default", Seq(factTable))

    insertKuduData(factTable, TestObjects.smallDatasetSeconds, KuduObjects.defaultFactSchema)

    insertKuduData(dimensionTable, KuduObjects.defaultDimData, KuduObjects.defaultDimSchema)

    insertKuduData(subdimTable, KuduObjects.defaultSubData, KuduObjects.defaultSubSchema)

    val subQualifiedTableName = createQualifiedTableName(database, subdimTable)

    val filter = new DatedTableFilter(database, factTable) with KuduStorage
    filter.doFilter(computeCountsFlag = false)

    val resultDf =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> subQualifiedTableName))
        .kudu

    assertResult(1)(resultDf.count())
  }

  test("Don't filter out child dataset with a hold") {
    val factTableName =
      createKuduTable(KuduObjects.defaultFactSchema, KuduObjects.defaultFactKey, kuduContext)
    val dimTableName =
      createKuduTable(KuduObjects.defaultDimSchema, KuduObjects.defaultDimKey, kuduContext)

    val dimensionTable: ChildTable =
      ChildTable(dimTableName, "kudu", KuduObjects.defaultFactJoin, Some(Hold(true, "legal", "legal@client.biz")), None)
    val factTable =
      DatedTable(factTableName, "kudu", "date", 1, None, None, Some(List(dimensionTable)))
    val database = Database("impala::default", Seq(factTable))

    insertKuduData(factTable, TestObjects.smallDatasetSeconds, KuduObjects.defaultFactSchema)

    insertKuduData(dimensionTable, KuduObjects.defaultDimData, KuduObjects.defaultDimSchema)

    val dimQualifiedTableName = s"${database.name}.${dimTableName}"

    val resultDf =
      spark.sqlContext.read
        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> dimQualifiedTableName))
        .kudu

    assertResult(3)(resultDf.count())
  }

  /*
   * createKuduTable creates a randomly named Kudu Table
   * And returns the random name it generated for the table
   */
  def createKuduTable(schema: StructType, primaryKey: Seq[String], kc: KuduContext): String = {
    val kuduTableName = UUID.randomUUID().toString().substring(0, 5)

    val qualifiedtableName = s"impala::default.${kuduTableName}"

    if (kc.tableExists(qualifiedtableName)) {
      kc.deleteTable(qualifiedtableName)
    }

    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.setRangePartitionColumns(List(primaryKey(0)).asJava).setNumReplicas(1)

    kc.createTable(qualifiedtableName, schema, primaryKey, kuduTableOptions)

    kuduTableName
  }

  /*
   * createQualifiedTableName creates a qualified name for testing
   */
  def createQualifiedTableName(database: Database, table: Table): String = {
    s"${database.name}.${table.name}"
  }

  /*
   * insertKuduData inserts test data into test kudu tables
   */
  def insertKuduData(table: Table, data: List[List[_]], schema: StructType): Unit = {
    val qualifiedTableName = s"impala::default.${table.name}"

    val df = createDataFrame(data, schema)

    kuduContext.insertRows(df, qualifiedTableName)
  }
}
