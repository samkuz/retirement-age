package io.phdata.retirementage.filters

import io.phdata.retirementage.domain.{CustomTable, Database}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.{Failure, Success, Try}

abstract class CustomTableFilter(database: Database, table: CustomTable)
    extends TableFilter(database, table) {

  override def expiredRecords(): DataFrame = {
    currentFrame.except(filteredFrame())
  }

  override def filteredFrame() = executeFilter()

  override def hasExpiredRecords(): Boolean = true

  def executeFilter(): DataFrame = {
    currentFrame
//    // add try catch block
//    if (table.filters.isDefined) {
//      var tempFrame = currentFrame
//      for (i <- table.filters.get) {
//        tempFrame.createOrReplaceTempView("tempFrame")
//        log(s"Executing SQL Query: SELECT * FROM tempFrame WHERE ${i.filter}")
//        tempFrame = tempFrame.sqlContext.sql(s"SELECT * FROM tempFrame WHERE ${i.filter}")
//      }
//      tempFrame
//    } else {
//      log("No custom filters defined")
//      currentFrame
//    }
  }
}
