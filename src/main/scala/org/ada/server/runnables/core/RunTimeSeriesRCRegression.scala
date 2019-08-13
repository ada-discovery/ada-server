package org.ada.server.runnables.core

import java.{lang => jl}

import javax.inject.Inject
import com.bnd.math.domain.rand.RandomDistribution
import com.bnd.network.domain.ActivationFunctionType
import org.ada.server.models.ml.IOJsonTimeSeriesSpec
import org.ada.server.dataaccess.RepoTypes.RegressorRepo
import org.ada.server.dataaccess.dataset.DataSetAccessorFactory
import reactivemongo.bson.BSONObjectID
import org.ada.server.services.ml.MachineLearningService
import org.incal.core.runnables.InputFutureRunnableExt
import org.incal.spark_ml.models.ValueOrSeq.ValueOrSeq
import org.incal.spark_ml.models.ReservoirSpec
import org.incal.spark_ml.models.regression.RegressionEvalMetric
import org.incal.spark_ml.models.setting.{RegressionLearningSetting, TemporalRegressionLearningSetting}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.runtime.universe.typeOf
import scala.concurrent.Future

class RunTimeSeriesRCRegression @Inject() (
    dsaf: DataSetAccessorFactory,
    mlService: MachineLearningService,
    regressionRepo: RegressorRepo
  ) extends InputFutureRunnableExt[RunTimeSeriesRCRegressionSpec] with TimeSeriesResultsHelper {

  override def runAsFuture(
    input: RunTimeSeriesRCRegressionSpec
  ): Future[Unit] = {
    println(input)

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
          TemporalRegressionLearningSetting(
            core = input.learningSetting,
            predictAhead = input.predictAhead,
            slidingWindowSize = Left(input.windowSize),
            reservoirSetting = Some(input.reservoirSpec),
            minCrossValidationTrainingSizeRatio = input.minCrossValidationTrainingSizeRatio,
            trainingTestSplitOrderValue = input.trainingTestSplitOrderValue
          ),
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

case class RunTimeSeriesRCRegressionSpec(
  // input/output specification
  dataSetId: String,
  itemId: BSONObjectID,
  ioSpec: IOJsonTimeSeriesSpec,
  predictAhead: Int,

  // ML model
  mlModelId: BSONObjectID,

  // delay line window size
  windowSize: Option[Int],

  // reservoir setting
  reservoirNodeNum: ValueOrSeq[Int] = Left(None),
  reservoirInDegree: ValueOrSeq[Int] = Left(None),
  reservoirEdgesNum: ValueOrSeq[Int] = Left(None),
  reservoirPreferentialAttachment: Boolean = false,
  reservoirBias: Boolean = false,
  reservoirCircularInEdges: Option[Seq[Int]] = None,
  inputReservoirConnectivity: ValueOrSeq[Double] = Left(None),
  reservoirSpectralRadius: ValueOrSeq[Double] = Left(None),
  reservoirFunctionType: ActivationFunctionType,
  reservoirFunctionParams: Seq[Double] = Nil,
  washoutPeriod: ValueOrSeq[Int] = Left(None),

  // cross-validation
  crossValidationFolds: Option[Int],
  crossValidationMinTrainingSizeRatio: Option[Double],
  crossValidationEvalMetric: Option[RegressionEvalMetric.Value],

  // learning setting
  learningSetting: RegressionLearningSetting,
  minCrossValidationTrainingSizeRatio: Option[Double],
  trainingTestSplitOrderValue: Option[Double],

  // replication item
  replicationItemId: Option[BSONObjectID]
) {
  def reservoirSpec =
    ReservoirSpec(
      inputNodeNum = learningSetting.pcaDims.getOrElse(ioSpec.inputSeriesFieldPaths.size) * windowSize.getOrElse(1),
      bias = 1,
      nonBiasInitial = 0,
      reservoirNodeNum = reservoirNodeNum,
      reservoirInDegree = reservoirInDegree,
      reservoirEdgesNum = reservoirEdgesNum,
      reservoirPreferentialAttachment = reservoirPreferentialAttachment,
      reservoirBias = reservoirBias,
      reservoirCircularInEdges = reservoirCircularInEdges,
      inputReservoirConnectivity = inputReservoirConnectivity,
      reservoirSpectralRadius = reservoirSpectralRadius,
      reservoirFunctionType = reservoirFunctionType,
      reservoirFunctionParams = reservoirFunctionParams,
      weightDistribution = RandomDistribution.createNormalDistribution(classOf[jl.Double], 0d, 1d),
      washoutPeriod = washoutPeriod
    )
}