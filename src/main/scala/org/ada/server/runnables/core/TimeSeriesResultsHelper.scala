package org.ada.server.runnables.core

import org.incal.core.{PlotSetting, PlotlyPlotter}
import org.incal.spark_ml.MLResultUtil
import org.incal.spark_ml.models.regression.RegressionEvalMetric
import org.incal.spark_ml.models.result.RegressionResultsHolder
import play.api.Logger

trait TimeSeriesResultsHelper {

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

    resultsHolder.expectedActualOutputs.headOption.map { outputs =>
      val trainingOutputs = outputs._1
      val testOutputs = outputs._2

//      println("Training")
//      println(trainingOutputs.map(_._1).mkString(","))
//      println(trainingOutputs.map(_._2).mkString(","))
//
//      println
//      println("Test")
//      println(testOutputs.map(_._1).mkString(","))
//      println(testOutputs.map(_._2).mkString(","))

      if (trainingOutputs.nonEmpty)
        exportOutputs(trainingOutputs, "training_io.html")

      if (testOutputs.nonEmpty)
        exportOutputs(testOutputs, "test_io.html")
    }
  }

  private def exportOutputs(
    outputs: Seq[(Double, Double)],
    fileName: String
  ) = {
    val y = outputs.map{ case (yhat, y) => y }
    val yhat = outputs.map{ case (yhat, y) => yhat }

    PlotlyPlotter.plotLines(
      Seq(y, yhat),
      Nil,
      PlotSetting(
        xLabel = Some("Time"),
        yLabel = Some("Value"),
        captions = Seq("Actual Output", "Expected Output")
      ),
      fileName
    )
  }
}
