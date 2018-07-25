/* Copyright 2018 phData Inc. */

package io.phdata.retirementage.storage

import io.phdata.retirementage.domain.RetirementReport
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import io.phdata.retirementage.SparkDriver.spark
import io.phdata.retirementage.domain.{DatasetReport, RetirementReport}
import org.apache.kudu.spark.kudu._

trait KuduStorage extends StorageActions with LazyLogging {
  override def persistFrame(computeCountsFlag: Boolean,
                            dryRun: Boolean,
                            qualifiedTableName: String,
                            storageType: String,
                            currentFrame: DataFrame,
                            filteredFrame: DataFrame): RetirementReport = {
    try {
      val kuduContext =
        new KuduContext("localhost:7051", spark.sqlContext.sparkContext)

      val currentDatasetCount = if (computeCountsFlag) Some(currentFrame.count()) else None

      val newDatasetCount =
        if (computeCountsFlag) Some(currentFrame.count() - filteredFrame.count()) else None

      if (!dryRun) {
        logger.info(s"deleting expired rows in $qualifiedTableName")
        val primaryKey = kuduContext.syncClient
          .openTable(qualifiedTableName)
          .getSchema
          .getPrimaryKeyColumns()
          .get(0)
          .getName
        // Selecting keys to delete from the kudu table
        val filteredKeys = filteredFrame.select(primaryKey)

        kuduContext.deleteRows(filteredKeys, qualifiedTableName)

        RetirementReport(qualifiedTableName,
                         true,
                         DatasetReport(qualifiedTableName, currentDatasetCount),
                         Some(DatasetReport(qualifiedTableName, newDatasetCount)),
                         None)
      }
      RetirementReport(
        qualifiedTableName,
        true,
        DatasetReport(qualifiedTableName, currentDatasetCount),
        Some(DatasetReport(qualifiedTableName, newDatasetCount)),
        None
      )
    } catch {
      case e: Exception => {
        logger.error(s"exception writing $qualifiedTableName", e)
        RetirementReport(qualifiedTableName,
                         false,
                         DatasetReport(getCurrentDatasetLocation(qualifiedTableName)),
                         None,
                         Some(e.getMessage))
      }
    }
  }

  override def getNewDatasetLocation(qualifiedTableName: String): String = {
    // No new dataset location for kudu tables
    qualifiedTableName
  }

  override def getCurrentDatasetLocation(qualifiedTableName: String): String = {
    qualifiedTableName
  }

  override def undo(qualifiedTableName: String): RetirementReport =
    throw new NotImplementedError("Cannot undo Kudu table deletes")
}
