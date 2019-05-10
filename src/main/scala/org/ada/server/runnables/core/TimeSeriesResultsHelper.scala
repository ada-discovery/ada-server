package org.ada.server.runnables.core

import com.banda.core.plotter.{Plotter, SeriesPlotSetting}
import org.incal.spark_ml.MLResultUtil
import org.incal.spark_ml.models.regression.RegressionEvalMetric
import org.incal.spark_ml.models.result.RegressionResultsHolder
import org.incal.core.util.writeStringAsStream
import play.api.Logger

trait TimeSeriesResultsHelper {

  private val plotter = Plotter("svg")
  private val logger = Logger

  protected def exportResults(resultsHolder: RegressionResultsHolder) = {
    // prepare the results stats
    val metricStatsMap = MLResultUtil.calcMetricStats(resultsHolder.performanceResults)

    val (rmseTrainingScore, rmseTestScore, _) = metricStatsMap.get(RegressionEvalMetric.rmse).get
    val (maeTrainingScore, maeTestScore, _) = metricStatsMap.get(RegressionEvalMetric.mae).get


    logger.info("Mean Training RMSE: " + rmseTrainingScore.mean)
    logger.info("Mean Training MAE : " + maeTrainingScore.mean)
    logger.info("-----------------------------")
    logger.info("Mean Test RMSE    : " + rmseTestScore.map(_.mean).getOrElse("N/A"))
    logger.info("Mean Test MAE     : " + maeTestScore.map(_.mean).getOrElse("N/A"))

    resultsHolder.expectedAndActualOutputs.headOption.map { outputs =>
      val trainingOutputs = outputs.head
      val testOutputs = outputs.tail.head

//      println("Training")
//      println(trainingOutputs.map(_._1).mkString(","))
//      println(trainingOutputs.map(_._2).mkString(","))
//
//      println
//      println("Test")
//      println(testOutputs.map(_._1).mkString(","))
//      println(testOutputs.map(_._2).mkString(","))

      if (trainingOutputs.nonEmpty)
        exportOutputs(trainingOutputs, "training_io.svg")

      if (testOutputs.nonEmpty)
        exportOutputs(testOutputs, "test_io.svg")
    }
  }

  private def exportOutputs(
    outputs: Seq[(Double, Double)],
    fileName: String
  ) = {
    val y = outputs.map{ case (yhat, y) => y }
    val yhat = outputs.map{ case (yhat, y) => yhat }

    val output = plotter.plotSeries(
      Seq(y, yhat),
      new SeriesPlotSetting()
        .setXLabel("Time")
        .setYLabel("Value")
        .setCaptions(Seq("Actual Output", "Expected Output"))
    )

    writeStringAsStream(output, new java.io.File(fileName))
  }
}
