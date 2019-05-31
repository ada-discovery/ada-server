package org.ada.server.runnables.core

import javax.inject.Inject
import org.apache.commons.lang3.StringEscapeUtils
import play.api.Logger
import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.services.StatsService

import scala.concurrent.ExecutionContext.Implicits.global

class CalcMetricMDSFromFile @Inject() (statsService: StatsService) extends InputFutureRunnableExt[CalcMetricMDSFromFileSpec] {

  private val logger = Logger
  private val defaultDelimiter = ","

  override def runAsFuture(input: CalcMetricMDSFromFileSpec) = {
    val delimiter = StringEscapeUtils.unescapeJava(input.delimiter.getOrElse(defaultDelimiter))

    for {
       // create a double-value file source and retrieve the field names
      (source, fieldNames) <- FeatureMatrixIO.load(input.inputFileName, Some(1), delimiter)

      // perform metric MDS
      (mdsProjections, eigenValues) <- statsService.performMetricMDS(source, input.dims, input.scaleByEigenValues)
    } yield {
      logger.info(s"Exporting the calculated MDS projections to ${input.exportFileName}.")
      FeatureMatrixIO.save(
        mdsProjections,
        fieldNames,
        for (i <- 1 to input.dims) yield "x" + i,
        "featureName",
        input.exportFileName,
        (value: Double) => value.toString,
        delimiter
      )
    }
  }
}

case class CalcMetricMDSFromFileSpec(
  inputFileName: String,
  delimiter: Option[String],
  dims: Int,
  scaleByEigenValues: Boolean,
  exportFileName: String
)