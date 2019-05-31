package org.ada.server.runnables.core

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.ada.server.runnables.core.CalcUtil._
import com.google.inject.Inject
import org.incal.core.dataaccess.Criterion._
import org.ada.server.models.{Field, FieldTypeId}
import org.incal.core.dataaccess.{AsyncReadonlyRepo, Criterion}
import org.ada.server.dataaccess.dataset.DataSetAccessorFactory
import play.api.Logger
import play.api.libs.json.JsObject
import reactivemongo.bson.BSONObjectID
import org.incal.core.runnables.{InputFutureRunnable, InputFutureRunnableExt}
import org.ada.server.services.StatsService

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.universe.typeOf

class CalcCorrelations @Inject()(
    dsaf: DataSetAccessorFactory,
    statsService: StatsService
  ) extends InputFutureRunnableExt[CalcCorrelationsSpec] {

  private val logger = Logger

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  import statsService._

  def runAsFuture(input: CalcCorrelationsSpec) = {
    val dsa = dsaf(input.dataSetId).get
    val dataSetRepo = dsa.dataSetRepo

    for {
      // get the fields first
      numericFields <- numericFields(dsa.fieldRepo)(input.featuresNum, input.allFeaturesExcept)

      // sorted fields
      sortedFields = numericFields.toSeq.sortBy(_.name)
      fieldNames = sortedFields.map(_.name)

      // calculate correlations standardly
      correlationsWithExecTime <- repeatWithTimeOptional(input.standardRepetitions) {
        dataSetRepo.find(projection = fieldNames).map(
          pearsonCorrelationExec.execJson((), sortedFields)
        )
      }

      // calculate correlations as a stream
      streamedCorrelationsWithExecTime <- repeatWithTimeOptional(input.streamRepetitions) {
        calcPearsonCorrelationsStreamed(dataSetRepo, Nil, sortedFields, input.streamParallelism, input.streamWithProjection, input.streamAreValuesAllDefined)
      }
    } yield {
      val (correlations, execTime) = correlationsWithExecTime.getOrElse((Nil, 0))
      val (streamedCorrelations, streamedExecTime) = streamedCorrelationsWithExecTime.getOrElse((Nil, 0))

      logger.info(s"Correlation for ${numericFields.size} fields using ALL DATA finished in ${execTime} sec on average.")
      logger.info(s"Correlation for ${numericFields.size} fields using STREAMS finished in ${streamedExecTime} sec on average.")

      // check if both results match
      correlations.zip(streamedCorrelations).map { case (rowCor1, rowCor2) =>
        rowCor1.zip(rowCor2).map { case (cor1, cor2) =>
          assert(cor1.equals(cor2), s"$cor1 is not equal $cor2.")
        }
      }

      val correlationsToExport = if (correlations.nonEmpty) correlations else streamedCorrelations
      input.exportFileName.map { exportFileName =>

        logger.info(s"Exporting the calculated correlations to $exportFileName.")

        FeatureMatrixIO.saveSquare(
          correlationsToExport,
          sortedFields.map(_.name),
          exportFileName,
          (value: Option[Double]) => value.map(_.toString).getOrElse("")
        )
      }.getOrElse(
        ()
      )
    }
  }

  private def calcPearsonCorrelationsStreamed(
    dataRepo: AsyncReadonlyRepo[JsObject, BSONObjectID],
    criteria: Seq[Criterion[Any]],
    fields: Seq[Field],
    parallelism: Option[Int],
    withProjection: Boolean,
    areValuesAllDefined: Boolean
  ): Future[Seq[Seq[Option[Double]]]] = {
    val exec =
      if (areValuesAllDefined)
        (pearsonCorrelationAllDefinedExec.execJsonRepoStreamed)_
      else
        (pearsonCorrelationExec.execJsonRepoStreamed)_

    exec(parallelism, parallelism, withProjection, fields)(dataRepo, criteria)
  }
}

case class CalcCorrelationsSpec(
  dataSetId: String,
  featuresNum: Option[Int],
  allFeaturesExcept: Seq[String],
  standardRepetitions: Int,
  streamRepetitions: Int,
  streamParallelism: Option[Int],
  streamWithProjection: Boolean,
  streamAreValuesAllDefined: Boolean,
  exportFileName: Option[String]
)