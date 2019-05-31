package org.ada.server.runnables.core

import javax.inject.Inject
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.commons.lang3.StringEscapeUtils
import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.services.StatsService

import scala.concurrent.ExecutionContext.Implicits.global

class CalcCorrelationsFromFile @Inject() (statsService: StatsService) extends InputFutureRunnableExt[CalcCorrelationsFromFileSpec] {

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()
  private val defaultDelimiter = ","

  override def runAsFuture(input: CalcCorrelationsFromFileSpec) = {
    val delimiter = StringEscapeUtils.unescapeJava(input.delimiter.getOrElse(defaultDelimiter))

    for {
      // create a double-value file source and retrieve the field names
      (source, fieldNames) <- FeatureMatrixIO.load(input.inputFileName, input.skipFirstColumns, delimiter)

      // calc correlations
      correlations <- statsService.pearsonCorrelationAllDefinedExec.execStreamed(input.streamParallelism, input.streamParallelism)(source)
    } yield
      FeatureMatrixIO.saveSquare(
        correlations,
        fieldNames,
        input.exportFileName,
        (value: Option[Double]) => value.map(_.toString).getOrElse(""),
        delimiter
      )
  }
}

case class CalcCorrelationsFromFileSpec(
  inputFileName: String,
  delimiter: Option[String],
  skipFirstColumns: Option[Int],
  streamParallelism: Option[Int],
  exportFileName: String
)