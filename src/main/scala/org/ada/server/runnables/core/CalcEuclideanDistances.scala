package org.ada.server.runnables.core

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
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
import org.ada.server.runnables.core.CalcUtil._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.universe.typeOf

class CalcEuclideanDistances @Inject()(
    val dsaf: DataSetAccessorFactory,
    val statsService: StatsService
  ) extends InputFutureRunnableExt[CalcEuclideanDistancesSpec] {

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  import statsService._

  private val logger = Logger

  def runAsFuture(input: CalcEuclideanDistancesSpec) = {
    val dsa = dsaf(input.dataSetId).get
    val dataSetRepo = dsa.dataSetRepo

    for {
      // get the fields first
      numericFields <- numericFields(dsa.fieldRepo)(input.featuresNum, input.allFeaturesExcept)

      // sorted fields
      sortedFields = numericFields.toSeq.sortBy(_.name)
      fieldNames = sortedFields.map(_.name)

      // calculate Euclidean distances standardly
      euclideanDistancesWithExecTime <- repeatWithTimeOptional(input.standardRepetitions) {
        dataSetRepo.find(projection = fieldNames).map(
          euclideanDistanceExec.execJson((), sortedFields)
        )
      }

      // calculate Euclidean distances as a stream
      streamedEuclideanDistancesWithExecTime <- repeatWithTimeOptional(input.streamRepetitions) {
        calcEuclideanDistanceStreamed(dataSetRepo, Nil, sortedFields, input.streamParallelism, input.streamWithProjection, input.streamAreValuesAllDefined)
      }
    } yield {
      val (euclideanDistances, execTime) = euclideanDistancesWithExecTime.getOrElse((Nil, 0))
      val (streamedEuclideanDistances, streamedExecTime) = streamedEuclideanDistancesWithExecTime.getOrElse((Nil, 0))

      logger.info(s"Euclidean distances for ${numericFields.size} fields using ALL DATA finished in ${execTime} sec on average.")
      logger.info(s"Euclidean distances for ${numericFields.size} fields using STREAMS finished in ${streamedExecTime} sec on average.")

      // check if both results match
      euclideanDistances.zip(streamedEuclideanDistances).map { case (rowCor1, rowCor2) =>
        rowCor1.zip(rowCor2).map { case (cor1, cor2) =>
          assert(cor1.equals(cor2), s"$cor1 is not equal $cor2.")
        }
      }

      val euclideanDistancesToExport = if (euclideanDistances.nonEmpty) euclideanDistances else streamedEuclideanDistances
      input.exportFileName.map { exportFileName =>

        logger.info(s"Exporting the calculated Euclidean distances to $exportFileName.")

        FeatureMatrixIO.saveSquare(
          euclideanDistancesToExport,
          sortedFields.map(_.name),
          exportFileName,
          (value: Double) => value.toString
        )
      }.getOrElse(
        ()
      )
    }
  }

  private def calcEuclideanDistanceStreamed(
    dataRepo: AsyncReadonlyRepo[JsObject, BSONObjectID],
    criteria: Seq[Criterion[Any]],
    fields: Seq[Field],
    parallelism: Option[Int] = None,
    withProjection: Boolean = true,
    areValuesAllDefined: Boolean = false
  ): Future[Seq[Seq[Double]]] = {
    val exec =
      if (areValuesAllDefined)
        (euclideanDistanceAllDefinedExec.execJsonRepoStreamed)_
      else
        (euclideanDistanceExec.execJsonRepoStreamed)_

    exec(parallelism, (), withProjection, fields)(dataRepo, criteria)
  }
}

case class CalcEuclideanDistancesSpec(
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