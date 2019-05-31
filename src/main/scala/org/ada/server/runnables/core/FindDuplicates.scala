package org.ada.server.runnables.core

import org.ada.server.models.DataSetFormattersAndIds.FieldIdentity
import play.api.Logger
import runnables.DsaInputFutureRunnable
import org.incal.core.dataaccess.Criterion._
import org.ada.server.field.FieldUtil.{FieldOps, JsonFieldOps}

import scala.reflect.runtime.universe.typeOf
import scala.concurrent.ExecutionContext.Implicits.global

class FindDuplicates extends DsaInputFutureRunnable[FindDuplicatesSpec] {

  private val logger = Logger

  override def runAsFuture(input: FindDuplicatesSpec) = {
    val dsa = createDsa(input.dataSetId)

    for {
      // get the items
      jsons <- dsa.dataSetRepo.find(projection = input.fieldNames)

      // get the fields
      fields <- dsa.fieldRepo.find(Seq(FieldIdentity.name #-> input.fieldNames))
    } yield {
      val fieldNameTypes = fields.map(_.toNamedTypeAny).toSeq
      val values = jsons.map(_.toValues(fieldNameTypes))

      val  duplicates = values.groupBy(identity).collect { case (x, Seq(_,_,_*)) => x }

      logger.info("Duplicates found: " + duplicates.size)
      logger.info("-----------------")
      logger.info(duplicates.mkString(", ") + "\n")
    }
  }
}

case class FindDuplicatesSpec(dataSetId: String, fieldNames: Seq[String])