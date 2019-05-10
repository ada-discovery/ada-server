package org.ada.server.runnables.core

import javax.inject.Inject
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.ada.server.calc.CalculatorExecutors
import org.apache.commons.lang3.StringEscapeUtils
import org.incal.core.runnables.InputFutureRunnable
import org.ada.server.services.StatsService

import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.runtime.universe.typeOf

class CalcEuclideanDistancesFromFile extends InputFutureRunnable[CalcEuclideanDistancesFromFileSpec] with CalculatorExecutors {

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()
  private val defaultDelimiter = ","

  override def runAsFuture(input: CalcEuclideanDistancesFromFileSpec) = {
    val delimiter = StringEscapeUtils.unescapeJava(input.delimiter.getOrElse(defaultDelimiter))

    for {
      // create a double-value file source and retrieve the field names
      (source, fieldNames) <- FeatureMatrixIO.load(input.inputFileName, input.skipFirstColumns, delimiter)

      // calc Euclidean distances
      distances <- euclideanDistanceAllDefinedExec.execStreamed(input.streamParallelism, input.streamParallelism)(source)
    } yield
      FeatureMatrixIO.saveSquare(
        distances,
        fieldNames,
        input.exportFileName,
        (value: Double) => value.toString,
        delimiter
      )
  }

  override def inputType = typeOf[CalcEuclideanDistancesFromFileSpec]
}

case class CalcEuclideanDistancesFromFileSpec(
  inputFileName: String,
  delimiter: Option[String],
  skipFirstColumns: Option[Int],
  streamParallelism: Option[Int],
  exportFileName: String
)