package org.ada.server.runnables.core

import javax.inject.Inject
import org.ada.server.models.ml.IOJsonTimeSeriesSpec
import org.ada.server.dataaccess.RepoTypes.RegressorRepo
import org.ada.server.dataaccess.dataset.{DataSetAccessor, DataSetAccessorFactory}
import reactivemongo.bson.BSONObjectID
import org.incal.core.runnables.InputFutureRunnableExt
import org.incal.spark_ml.models.VectorScalerType
import org.ada.server.services.ml.MachineLearningService
import org.incal.spark_ml.models.regression.RegressionEvalMetric
import org.incal.spark_ml.models.setting.{TemporalGroupIOSpec, TemporalRegressionLearningSetting}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.runtime.universe.typeOf
import scala.concurrent.Future

class RunTimeSeriesDLRegression @Inject() (
    dsaf: DataSetAccessorFactory,
    mlService: MachineLearningService,
    regressionRepo: RegressorRepo
  ) extends InputFutureRunnableExt[RunTimeSeriesDLRegressionSpec] with TimeSeriesResultsHelper {

  override def runAsFuture(
    input: RunTimeSeriesDLRegressionSpec
  ): Future[Unit] = {
    val dsa = dsaf(input.dataSetId).get

    for {
      // load a ML model
      mlModel <- regressionRepo.get(input.mlModelId)

      // main item
      item <- dsa.dataSetRepo.get(input.itemId)

      // replication item
      replicationItem <- input.replicationItemId.map { replicationId =>
        dsa.dataSetRepo.get(replicationId)
      }.getOrElse(
        Future(None)
      )

      // run the selected classifier (ML model)
      resultsHolder <- mlModel.map { mlModel =>
        val results = mlService.regressTemporalSeries(
          item.get,
          input.ioSpec,
          mlModel,
          input.learningSetting,
          replicationItem
        )
        results.map(Some(_))
      }.getOrElse(
        Future(None)
      )
    } yield
      resultsHolder.foreach(exportResults)
  }
}

case class RunTimeSeriesDLRegressionSpec(
  dataSetId: String,
  itemId: BSONObjectID,
  ioSpec: IOJsonTimeSeriesSpec,
  mlModelId: BSONObjectID,
  learningSetting: TemporalRegressionLearningSetting,
  replicationItemId: Option[BSONObjectID]
)


