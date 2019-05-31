package org.ada.server.runnables.core

import runnables.DsaInputFutureRunnable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.runtime.universe.typeOf

class ReplaceDotWithUnderScoreInLabels extends DsaInputFutureRunnable[ReplaceDotWithUnderScoreInLabelsSpec] {

  override def runAsFuture(spec: ReplaceDotWithUnderScoreInLabelsSpec) = {
    val fieldRepo = createDsa(spec.dataSetId).fieldRepo

    for {
      // get all the fields
      fields <- fieldRepo.find()

      _ <- {
        val newFields = fields.map { field =>
          val newLabel = field.label.map(_.replaceAllLiterally("u002e", "_"))
          field.copy(label = newLabel)
        }
        fieldRepo.update(newFields)
      }
    } yield
      ()
  }
}

case class ReplaceDotWithUnderScoreInLabelsSpec(dataSetId: String)