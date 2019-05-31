package org.ada.server.runnables.core

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.commons.lang3.StringEscapeUtils
import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.calc.CalculatorExecutors

import scala.concurrent.ExecutionContext.Implicits.global
import org.ada.server.calc.CalculatorHelper._

class CalcVariancesFromFile extends InputFutureRunnableExt[CalcVariancesFromFileSpec] with CalculatorExecutors {

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()
  private val defaultDelimiter = ","

  override def runAsFuture(input: CalcVariancesFromFileSpec) = {
    val delimiter = StringEscapeUtils.unescapeJava(input.delimiter.getOrElse(defaultDelimiter))

    for {
      // create a double-value file source and retrieve the field names
      (source, fieldNames) <- FeatureMatrixIO.load(input.inputFileName, input.skipFirstColumns, delimiter)

      optionalSource = source.map(_.map(Some(_)))

      // calc basic stats
      basicStats <- multiBasicStatsSeqExec.execStreamed_(optionalSource)
    } yield
      FeatureMatrixIO.save(
        basicStats.map(stats => Seq(stats.map(_.variance))),
        fieldNames,
        Seq("variance"),
        "fieldName",
        input.exportFileName,
        (value: Option[Double]) => value.map(_.toString).getOrElse(""),
        delimiter
      )
  }
}

case class CalcVariancesFromFileSpec(
  inputFileName: String,
  delimiter: Option[String],
  skipFirstColumns: Option[Int],
  streamParallelism: Option[Int],
  exportFileName: String
)