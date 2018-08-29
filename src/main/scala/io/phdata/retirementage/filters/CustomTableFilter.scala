package io.phdata.retirementage.filters

import com.amazonaws.services.kinesis.model.InvalidArgumentException
import io.phdata.retirementage.domain.{CustomTable, Database}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

abstract class CustomTableFilter(database: Database, table: CustomTable)
    extends TableFilter(database, table) {

  override def expiredRecords(): DataFrame = {
    currentFrame.except(filteredFrame())
  }

  override def filteredFrame() = executeFilter()

  override def hasExpiredRecords(): Boolean = true

  def executeFilter(): DataFrame = {
    try {
      var tempFrame = currentFrame
      for (i <- table.filters) {
        tempFrame.createOrReplaceTempView("tempFrame")
        log(s"Executing SQL Query: SELECT * FROM tempFrame WHERE NOT ${i.filter}")
        tempFrame = tempFrame.sqlContext.sql(s"SELECT * FROM tempFrame WHERE NOT ${i.filter}")
      }
      tempFrame
    } catch {
      case _: Throwable => throw new InvalidArgumentException("No filters found")
    }
  }
}
