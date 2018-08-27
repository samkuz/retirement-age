import io.phdata.retirementage.domain._
import net.jcazevedo.moultingyaml._
import io.phdata.retirementage.YamlProtocols.{lazyFormat, yamlFormat2, yamlFormat3, yamlFormat5, yamlFormat7, _}

object YamlProtocols extends DefaultYamlProtocol {
  implicit val joinKeys = yamlFormat2(JoinOn)
  // Make the yamlformat lazy to allow for recursive Table types
  implicit val datedTable: YamlFormat[DatedTable]   = lazyFormat(yamlFormat7(DatedTable))
  implicit val relatedTable: YamlFormat[ChildTable] = lazyFormat(yamlFormat5(ChildTable))
  implicit val customTable: YamlFormat[CustomTable] = lazyFormat(yamlFormat5(CustomTable))
  implicit val hold                                 = yamlFormat3(Hold)

  implicit val tableFormat    = TableYamlFormat
  implicit val databaseFormat = yamlFormat2(Database)
  implicit val configFormat   = yamlFormat2(Config)
  implicit val datasetReport  = yamlFormat2(DatasetReport)
  implicit val resultFormat   = yamlFormat5(RetirementReport)

  implicit object TableYamlFormat extends YamlFormat[Table] {
    def write(t: Table) = {
      t match {
        case d: DatedTable =>
          YamlObject(
            YamlString("name")              -> YamlString(d.name),
            YamlString("storage_type")      -> YamlString(d.storage_type),
            YamlString("expiration_column") -> YamlString(d.expiration_column),
            YamlString("expiration_days")   -> YamlNumber(d.expiration_days),
            YamlString("hold")              -> YamlNull,
            YamlString("child_tables")      -> YamlNull
          )
        case c: CustomTable =>
          YamlObject(
            YamlString("name")         -> YamlString(c.name),
            YamlString("storage_type") -> YamlString(c.storage_type),
            YamlString("filters")      -> YamlString("filter")
          )
      }
    }

    def read(value: YamlValue) = {
      value.asYamlObject.getFields(
        YamlString("name"),
        YamlString("storage_type"),
        YamlString("expiration_column"),
        YamlString("expiration_days")
      ) match {
        case Seq(YamlString(name),
        YamlString(storage_type),
        YamlString(expiration_column),
        YamlNumber(expiration_days)) =>
          DatedTable(name, storage_type, expiration_column, expiration_days.toInt, None, None, None)
        case Seq(YamlString(name), YamlString(storage_type), YamlString(filters)) =>
          CustomTable(name, storage_type, filters, None, None)
      }
    }
  }

}

val yamlTest = DatedTable("test1", "parquet", "col1", 10, None, None, None).toYaml

val table1 = yamlTest.convertTo[DatedTable]

val yamlTest2 = CustomTable("test2", "parquet", "misc", None, None).toYaml

val table2 = yamlTest2.converTo[CustomTable]

//val file =
//  """
//    |databases:
//    |  - name: database1
//    |    tables:
//    |      - name: parquet1
//    |        storage_type: parquet
//    |        expiration_column: col1
//    |        expiration_days: 10
//    |        hold:
//    |        child_tables:
//    |      - name: kudu1
//    |        storage_type: kudu
//    |        filters:
//    |        hold: false
//    |        child_tables:
//  """.stripMargin
//
//val test = file.parseYaml
//
//println(test)
