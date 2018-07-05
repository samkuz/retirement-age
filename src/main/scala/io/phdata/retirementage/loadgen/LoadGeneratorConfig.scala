/* Copyright 2018 phData Inc. */

package io.phdata.retirementage.loadgen

import org.rogach.scallop.ScallopConf

class LoadGeneratorConfig(args: Array[String]) extends ScallopConf(args) {
  val factCount         = opt[Int]("fact-count", required = true, default = Some(0))
  val dimensionCount    = opt[Int]("dimension-count", required = true, default = Some(0))
  val subDimensionCount = opt[Int]("subdimension-count", required = true, default = Some(0))
  val databaseName      = opt[String]("database-name", required = true, default = Some("default"))

  val factName = opt[String]("fact-name", required = false, default = Some("factLoadTest"))
  val dimName = opt[String]("dim-name", required = false, default = Some("dimLoadTest"))
  val subName = opt[String]("subdim-name", required = false, default = Some("subLoadTest"))

  verify()
}
