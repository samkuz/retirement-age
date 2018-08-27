package io.phdata.retirementage.domain

case class CustomTable(name: String,
                       storage_type: String,
                       filters: Option[List[String]],
                       hold: Option[Hold],
                       child_tables: Option[List[ChildTable]])
    extends Table
