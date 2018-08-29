package io.phdata.retirementage.filters

import io.phdata.retirementage.domain._
import io.phdata.retirementage.storage.HdfsStorage
import io.phdata.retirementage.{SparkTestBase, TestObjects}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import java.sql.Date

class CustomTableFilterTest extends FunSuite with SparkTestBase {
  test("Filter out a date when column is unix time seconds") {

    val table    = CustomTable("table1", "parquet", List(CustomFilter(s"date = ${Date.valueOf("2018-12-25").getTime / 1000}")), None, None)
    val database = Database("default", Seq(table))
    val schema   = StructType(StructField("date", LongType, false) :: Nil)
    val data     = TestObjects.smallDatasetSeconds

    val df = createDataFrame(TestObjects.smallDatasetSeconds, schema)

    val filter = getCustomFrameFilter(database, table, df)

    assertResult(1)(filter.expiredRecordsCount())
    assertResult(2)(filter.newDatasetCount())
  }

  def getCustomFrameFilter(database: Database, table: CustomTable, frame: DataFrame) = {
    class TestTableFilter(database: Database, table: CustomTable)
      extends CustomTableFilter(database, table)
        with HdfsStorage {

      //override lazy val currentFrame: DataFrame = frame

      override def removeRecords(computeCountsFlag: Boolean,
                                 dryRun: Boolean,
                                 qualifiedTableName: String,
                                 storageType: String,
                                 currentFrame: DataFrame,
                                 filteredFrame: DataFrame): RetirementReport = {

        RetirementReport(
          qualifiedTableName,
          true,
          DatasetReport("", Some(currentDatasetCount)),
          Some(
            DatasetReport(getCurrentDatasetLocation(s"${database.name}.${table.name}"),
              Some(filteredFrame.count()))),
          None
        )
      }
    }

    new TestTableFilter(database, table)
  }
}
