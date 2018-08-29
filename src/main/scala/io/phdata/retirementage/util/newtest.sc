import io.phdata.retirementage.domain._
import net.jcazevedo.moultingyaml._

object MyYamlProtocols extends DefaultYamlProtocol {
  implicit val joinKeys = yamlFormat2(JoinOn)
  // Make the yamlformat lazy to allow for recursive Table types
  implicit val datedTable: YamlFormat[DatedTable]   = lazyFormat(yamlFormat7(DatedTable))
  implicit val relatedTable: YamlFormat[ChildTable] = lazyFormat(yamlFormat5(ChildTable))
  implicit val hold                                 = yamlFormat3(Hold)

  implicit val customTable: YamlFormat[CustomTable] = lazyFormat(yamlFormat5(CustomTable))
  implicit val tableFormat: YamlFormat[Table] = TableYamlFormat

  implicit val databaseFormat                 = yamlFormat2(Database)
  implicit val configFormat                   = yamlFormat2(Config)
  implicit val datasetReport                  = yamlFormat2(DatasetReport)
  implicit val resultFormat                   = yamlFormat5(RetirementReport)

  implicit object TableYamlFormat extends YamlFormat[Table] {
        def write(t: Table) = {
          t match {
            case d: DatedTable =>
              YamlObject(
                YamlString("name")              -> YamlString(d.name),
                YamlString("storage_type")      -> YamlString(d.storage_type),
                YamlString("expiration_column") -> YamlString(d.expiration_column),
                YamlString("expiration_days")   -> YamlNumber(d.expiration_days),
                YamlString("hold")              -> d.hold.toYaml,
                YamlString("date_format_string")->(
                  if(d.date_format_string.isDefined){
                    YamlString(d.date_format_string.get)
                  }else{
                    YamlNull
                  }),
                YamlString("child_tables")      -> d.child_tables.toYaml
              )
            case c: CustomTable =>
              YamlObject(
                YamlString("name")         -> YamlString(c.name),
                YamlString("storage_type") -> YamlString(c.storage_type),
                YamlString("filters")      -> YamlString(c.filters),
                YamlString("hold") -> c.hold.toYaml,
                YamlString("child_tables")      -> c.child_tables.toYaml
              )
          }
        }

        def read(value: YamlValue) = {
//          try{
//            value.asYamlObject.getFields(
//              YamlString("name"),
//              YamlString("storage_type"),
//              YamlString("expiration_column"),
//              YamlString("expiration_days")
//            ) match {
//              case Seq(YamlString(name),
//              YamlString(storage_type),
//              YamlString(expiration_column),
//              YamlNumber(expiration_days)) =>
//                DatedTable(name, storage_type,
//                  expiration_column, expiration_days.toInt, None, None, None)
//            }
//          }catch{
//            case _: Throwable =>
//              value.asYamlObject.getFields(
//                YamlString("name"),
//                YamlString("storage_type"),
//                YamlString("filters")
//              ) match {
//                case Seq(YamlString(name), YamlString(storage_type), YamlString(filters)) =>
//                  CustomTable(name, storage_type, filters, None, None)
//              }
//          }
        try{
          value.asYamlObject.convertTo[DatedTable]
        }catch{
          case _: Throwable => value.asYamlObject.convertTo[CustomTable]
        }

        }
      }
}

import MyYamlProtocols._

val yamlTest = DatedTable("test1", "parquet", "col1", 10, Some(Hold(true, "no reason", "sam")), None, Some(List(ChildTable("test_child", "parquet", JoinOn("col1", "col1"), None, None)))).toYaml

val table1 = yamlTest.convertTo[DatedTable]

val yamlTest2 = CustomTable("test2", "parquet", "misc", None, None).toYaml

val table2 = yamlTest2.convertTo[CustomTable]

val file =
  """
    |databases:
    |  - name: database1
    |    tables:
    |      - name: parquet1
    |        storage_type: parquet
    |        expiration_column: col1
    |        expiration_days: 10
    |        hold:
    |        child_tables:
    |          - name: parquet2
    |            storage_type: parquet
    |            join_on:
    |              parent: col1
    |              self: col1
    |      - name: kudu1
    |        storage_type: kudu
    |        filters: 'misc'
    |        hold:
    |        child_tables:
  """.stripMargin

val test = file.parseYaml

val testConfig = test.convertTo[Config]
