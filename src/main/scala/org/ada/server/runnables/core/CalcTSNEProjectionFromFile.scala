package org.ada.server.runnables.core

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.google.inject.Inject
import org.ada.server.AdaException
import org.apache.commons.lang3.StringEscapeUtils
import org.ada.server.dataaccess.dataset.DataSetAccessorFactory
import play.api.Logger
import org.incal.core.{PlotSetting, PlotlyPlotter}
import org.incal.core.runnables.{InputFutureRunnable, InputFutureRunnableExt}
import org.ada.server.services.{StatsService, TSNESetting}

import scala.concurrent.ExecutionContext.Implicits.global

class CalcTSNEProjectionFromFile @Inject()(
    dsaf: DataSetAccessorFactory,
    statsService: StatsService
  ) extends InputFutureRunnableExt[CalcTSNEProjectionFromFileSpec] {

  import statsService._

  private val logger = Logger

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()
  private val defaultDelimiter = ","

  def runAsFuture(input: CalcTSNEProjectionFromFileSpec) = {
    val delimiter = StringEscapeUtils.unescapeJava(input.delimiter.getOrElse(defaultDelimiter))

    for {
      // create a double-value file source and retrieve the field names
      (source, fieldNames) <- FeatureMatrixIO.loadArrayWithFirstIdColumn(input.inputFileName, delimiter)

      // fully load everything from the source
      idInputs <- source.runWith(Sink.seq)
    } yield {

      // aux function
      def withDefault[T](default: T)(values: Seq[T]) =
        values match {
          case Nil => Seq(default)
          case _ => values
        }

      for {
        perplexity <- withDefault(20d)(input.perplexities)
        theta <- withDefault(0.5d)(input.thetas)
        iterations <- withDefault(1000)(input.iterations)
      } yield {
        val ids = idInputs.map(_._1)
        val arrayInput = idInputs.map(_._2).toArray
        val inputs = if (input.isColumnBased) arrayInput.transpose else arrayInput

        // prepare the setting
        val setting = TSNESetting(
          dims = input.dims,
          pcaDims = input.pcaDims,
          maxIterations = iterations,
          perplexity = perplexity,
          theta = theta
        )

        val pcaDimsPart = setting.pcaDims.map(pcaDims => s"_pca-$pcaDims").getOrElse("")
        val exportFileName = s"${input.exportFileName}-${input.dims}d_iter-${iterations}_per-${perplexity}_theta-${theta}" + pcaDimsPart
        val plotExportFileName = if (input.exportPlot) Some(exportFileName + ".html") else None

        runAndExportAux(
          input.inputFileName,
          inputs,
          ids,
          fieldNames,
          input.isColumnBased)(
          setting,
          exportFileName + ".csv",
          plotExportFileName,
          delimiter
        )
      }
    }
  }

  private def runAndExportAux(
    inputFileName: String,
    inputs: Array[Array[Double]],
    ids: Seq[String],
    fieldNames: Seq[String],
    isColumnBased: Boolean)(
    setting: TSNESetting,
    exportFileName: String,
    plotExportFileName: Option[String],
    delimiter: String
  ) = {
    // run t-SNE
    val tsneProjections = performTSNE(inputs, setting)

    val prefix = if (isColumnBased) "Column-based" else "Row-based"
    logger.info(s"$prefix t-SNE for a file ${inputFileName} finished.")

    // image export
    if (plotExportFileName.isDefined) {
      val tsneFailed = tsneProjections.exists(_.exists(_.isNaN))
      if (tsneFailed)
        logger.error(s"$prefix t-SNE for a file ${inputFileName} returned NaN values. Image export is not possible.")
      else {
        val xys = tsneProjections.map(data => (data(0), data(1))).toSeq
        PlotlyPlotter.plotScatter(Seq(xys), PlotSetting(title = Some("t-SNE")), plotExportFileName.get)
      }
    }

    if (isColumnBased) {
      if (tsneProjections.length != fieldNames.size - 1)
        throw new AdaException(s"The number of rows from $prefix t-SNE ${tsneProjections.length} is not equal to the number of features ${fieldNames.size - 1}")

      // save the results for columns
      FeatureMatrixIO.save(
        tsneProjections.map(_.toSeq),
        fieldNames.tail,
        for (i <- 1 to setting.dims) yield "x" + i,
        "featureName",
        exportFileName,
        (value: Double) => value.toString,
        delimiter
      )
    } else {
      if (tsneProjections.length != ids.size)
        throw new AdaException(s"The number of rows from $prefix t-SNE ${tsneProjections.length} is not equal to the number of ids/labels ${ids.size - 1}")

      // save the results for rows
      FeatureMatrixIO.save(
        tsneProjections.map(_.toSeq),
        ids,
        for (i <- 1 to setting.dims) yield "x" + i,
        fieldNames.head,
        exportFileName,
        (value: Double) => value.toString,
        delimiter
      )
    }
  }
}

case class CalcTSNEProjectionFromFileSpec(
  inputFileName: String,
  delimiter: Option[String],
  dims: Int,
  iterations: Seq[Int],
  perplexities: Seq[Double],
  thetas: Seq[Double],
  pcaDims: Option[Int],
  isColumnBased: Boolean,
  exportFileName: String,
  exportPlot: Boolean
)