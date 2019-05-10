package org.ada.server.runnables.core

import org.ada.server.models.DataSetFormattersAndIds.FieldIdentity
import play.api.Logger
import runnables.DsaInputFutureRunnable
import org.incal.core.dataaccess.Criterion._
import org.ada.server.field.FieldUtil.{FieldOps, JsonFieldOps}

import scala.reflect.runtime.universe.typeOf
import scala.concurrent.ExecutionContext.Implicits.global

class CountDistinct extends DsaInputFutureRunnable[CountDistinctSpec] {

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
        logger.info(value.mkString(", ") + " : " + items.size)
      }
    }
  }

  override def inputType = typeOf[CountDistinctSpec]
}

case class CountDistinctSpec(dataSetId: String, fieldNames: Seq[String])