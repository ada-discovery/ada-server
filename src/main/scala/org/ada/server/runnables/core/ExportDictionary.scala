package org.ada.server.runnables.core

import java.io.{File, PrintWriter}

import play.api.Logger
import runnables.DsaInputFutureRunnable
import org.ada.server.dataaccess.JsonUtil
import org.incal.core.dataaccess.AscSort
import play.api.libs.json.{JsObject, JsString, Json}

import scala.reflect.runtime.universe.typeOf
import scala.concurrent.ExecutionContext.Implicits.global

class ExportDictionary extends DsaInputFutureRunnable[ExportDictionarySpec] {

  private val logger = Logger // (this.getClass())
  private val delimiter = "\t"

  override def runAsFuture(input: ExportDictionarySpec) = {
    val fieldRepo = createDsa(input.dataSetId).fieldRepo

    for {
      // get the fields
      fields <- fieldRepo.find(sort = Seq(AscSort("name")))
    } yield {
      // collect all the lines
      val lines = fields.map { field =>
        val enumValuesString =
          if (field.numValues.nonEmpty) {
            val fields = field.numValues.map { case (a, b) => a -> JsString(b)}
            Json.stringify(JsObject(fields))
          } else ""

        Seq(JsonUtil.unescapeKey(field.name), field.label.getOrElse(""), field.fieldType.toString, enumValuesString).mkString(delimiter)
      }

      // create a header
      val header = Seq("name", "label", "fieldType", "enumValues").mkString(delimiter)

      // write to file
      val pw = new PrintWriter(new File(input.dataSetId + "_dictionary.tsv"))
      pw.write(header + "\n")
      pw.write(lines.mkString("\n"))
      pw.close
    }
  }
}

case class ExportDictionarySpec(dataSetId: String)