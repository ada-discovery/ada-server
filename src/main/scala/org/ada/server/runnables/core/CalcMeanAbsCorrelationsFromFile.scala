package org.ada.server.runnables.core

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.apache.commons.lang3.StringEscapeUtils
import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.calc.CalculatorExecutors

import scala.concurrent.ExecutionContext.Implicits.global

class CalcMeanAbsCorrelationsFromFile extends InputFutureRunnableExt[CalcMeanAbsCorrelationsFromFileSpec] with CalculatorExecutors {

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()
  private val defaultDelimiter = ","

  override def runAsFuture(input: CalcMeanAbsCorrelationsFromFileSpec) = {
    val delimiter = StringEscapeUtils.unescapeJava(input.delimiter.getOrElse(defaultDelimiter))

    for {
      // create a double-value file source and retrieve the field names
      (source, fieldNames) <- FeatureMatrixIO.loadWithFirstIdColumn(input.correlationsInputFileName, delimiter)

      // get the results
      results <- source.map { case (_, corrs) =>
        (corrs.map(Math.abs(_)).sum - 1) / (corrs.size - 1)
      }.runWith(Sink.seq)
    } yield
      FeatureMatrixIO.save(
        results.map(Seq(_)),
        fieldNames.tail,
        Seq("meanAsbCorrelation"),
        fieldNames.head,
        input.exportFileName,
        (value: Double) => value.toString,
        delimiter
      )
  }
}

case class CalcMeanAbsCorrelationsFromFileSpec(
  correlationsInputFileName: String,
  delimiter: Option[String],
  exportFileName: String
)