package org.ada.server.runnables.core

import javax.inject.Inject
import play.api.Logger
import org.incal.core.runnables.InputFutureRunnableExt
import org.incal.spark_ml.models.result.{ClassificationResult, StandardClassificationResult, TemporalClassificationResult}
import org.ada.server.dataaccess.dataset.ClassificationResultRepoFactory

import scala.reflect.runtime.universe.typeOf
import scala.concurrent.ExecutionContext.Implicits.global

class RemoveClassificationBinCurves @Inject()(repoFactory: ClassificationResultRepoFactory) extends InputFutureRunnableExt[RemoveClassificationBinCurvesSpec] {

  private val logger = Logger // (this.getClass())

  override def runAsFuture(input: RemoveClassificationBinCurvesSpec) = {
    val repo = repoFactory(input.dataSetId)

    for {
      // get all the results
      allResults <- repo.find()

      _ <- {
        val newResults = allResults.map { result =>
          result match {
            case result: StandardClassificationResult => result.copy(trainingBinCurves = Nil, testBinCurves = Nil, replicationBinCurves = Nil)
            case result: TemporalClassificationResult => result.copy(trainingBinCurves = Nil, testBinCurves = Nil, replicationBinCurves = Nil)
          }
        }
        repo.update(newResults)
      }
    } yield
      ()
  }
}

case class RemoveClassificationBinCurvesSpec(dataSetId: String)