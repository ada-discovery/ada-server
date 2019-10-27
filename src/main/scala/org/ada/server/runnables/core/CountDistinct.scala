package org.ada.server.runnables.core

import org.ada.server.models.DataSetFormattersAndIds.FieldIdentity
import play.api.Logger
import runnables.DsaInputFutureRunnable
import org.incal.core.dataaccess.Criterion._
import org.ada.server.field.FieldUtil.{FieldOps, JsonFieldOps}
import org.incal.core.runnables.RunnableHtmlOutput

import scala.concurrent.ExecutionContext.Implicits.global

class CountDistinct extends DsaInputFutureRunnable[CountDistinctSpec] with RunnableHtmlOutput {

  private val logger = Logger // (this.getClass())

  override def runAsFuture(input: CountDistinctSpec) = {
    val dsa = createDsa(input.dataSetId)

    for {
      // get the items
      jsons <- dsa.dataSetRepo.find(projection = input.fieldNames)

      // get the fields
      fields <- dsa.fieldRepo.find(Seq(FieldIdentity.name #-> input.fieldNames))
    } yield {
      val fieldNameTypes = fields.map(_.toNamedType).toSeq
      val values = jsons.map(_.toValues(fieldNameTypes))

      val distinctValues = values.groupBy(identity)

      logger.info("Distinct values found: " + distinctValues.size)
      logger.info("-----------------")

      distinctValues.foreach { case (value, items) =>
        val line = value.mkString(", ") + " : " + items.size
        logger.info(line)
        addParagraph(line)
      }
    }
  }
}

case class CountDistinctSpec(dataSetId: String, fieldNames: Seq[String])