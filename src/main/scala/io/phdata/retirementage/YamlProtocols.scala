/*
 * Copyright 2018 phData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.phdata.retirementage

import io.phdata.retirementage.domain._
import net.jcazevedo.moultingyaml.{DefaultYamlProtocol, YamlArray, YamlFormat, YamlObject, YamlString}

/**
  * Yaml protocols for parsing yaml configuration into domain objects using MoultingYaml.
  *
  */
object YamlProtocols extends DefaultYamlProtocol {
  implicit val joinKeys = yamlFormat2(JoinOn)
  // Make the yamlformat lazy to allow for recursive Table types
  implicit val datedTable: YamlFormat[DatedTable]   = lazyFormat(yamlFormat7(DatedTable))
  implicit val relatedTable: YamlFormat[ChildTable] = lazyFormat(yamlFormat5(ChildTable))
  implicit val hold                                 = yamlFormat3(Hold)

  implicit val databaseFormat = yamlFormat2(Database)
  implicit val configFormat   = yamlFormat2(Config)
  implicit val datasetReport  = yamlFormat2(DatasetReport)
  implicit val resultFormat   = yamlFormat5(RetirementReport)

  implicit object TableYamlFormat extends YamlFormat[Database] {
    def write(t: Table) = {
      t match{
        case _: DatedTable => YamlArray(YamlString(t.name))

        case _: CustomTable =>YamlArray(YamlString(t.name), YamlString(t.storage_type), YamlArray)
      }
    }

    )

    //      YamlObject(
    //      YamlString("name") -> YamlString(c.name),
    //      YamlString("red") -> YamlNumber(c.red),
    //      YamlString("green") -> YamlNumber(c.green),
    //      YamlString("blue") -> YamlNumber(c.blue)
    //    )
    def read(value: YamlValue) = {
      //      value.asYamlObject.getFields(
      //        YamlString("name"),
      //        YamlString("red"),
      //        YamlString("green"),
      //        YamlString("blue")) match {
      //        case Seq(
      //        YamlString(name),
      //        YamlNumber(red: Int),
      //        YamlNumber(green: Int),
      //        YamlNumber(blue: Int)) =>
      //          new Color(name, red, green, blue)
      //        case _ => deserializationError("Color expected")
      //      }
    }
  }
}

