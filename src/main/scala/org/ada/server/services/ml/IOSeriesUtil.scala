package org.ada.server.services.ml

import java.{lang => jl}
import org.ada.server.models.ml.IOJsonTimeSeriesSpec
import org.ada.server.dataaccess.JsonUtil
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.incal.spark_ml.models.VectorScalerType
import org.incal.spark_ml.transformers.VectorColumnScaler
import play.api.libs.json.JsObject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object IOSeriesUtil {

  def apply(
    json: JsObject,
    ioSpec: IOJsonTimeSeriesSpec
  ): Option[(Seq[Seq[Double]], Seq[Double])] = {
    // helper method to extract series for a given path
    def extractSeries(path: String): Option[Seq[Double]] = {
      val jsValues = JsonUtil.traverse(json, path)

      val leftTrimmedJsValues =
        ioSpec.dropLeftLength.map(jsValues.drop).getOrElse(jsValues)

      val trimmedJsValues = ioSpec.seriesLength match {
        case Some(length) =>
          val series = leftTrimmedJsValues.take(length)
          if (series.size == length) Some(series) else None

        case None =>
          val series = ioSpec.dropRightLength.map(leftTrimmedJsValues.dropRight).getOrElse(leftTrimmedJsValues)
          Some(series)
      }

      trimmedJsValues.map(_.map(_.as[Double]))
    }

    // extract input series
    val inputSeriesAux = ioSpec.inputSeriesFieldPaths.flatMap(extractSeries)

    val inputSeries =
      if (inputSeriesAux.size == ioSpec.inputSeriesFieldPaths.size)
        Some(inputSeriesAux.transpose)
      else
        None

    // extract output series
    val outputSeries = extractSeries(ioSpec.outputSeriesFieldPath)

    (inputSeries, outputSeries).zipped.headOption
  }

  def scaleSeriesJava(
    session: SparkSession)(
    series: Seq[Seq[jl.Double]],
    transformType: VectorScalerType.Value
  ): Future[Seq[Seq[jl.Double]]] = {
    val javaSeries: Seq[Seq[Double]] = series.map(_.map(_.toDouble))

    for {
      outputSeries <- scaleSeries(session)(javaSeries, transformType)
    } yield
      outputSeries.map(_.map(jl.Double.valueOf(_)))
  }

  def scaleSeries(
    session: SparkSession)(
    inputSeries: Seq[Seq[Double]],
    transformType: VectorScalerType.Value
  ): Future[Seq[Seq[Double]]] = {
    val rows = inputSeries.zipWithIndex.map { case (oneSeries, index) =>
      (index, Vectors.dense(oneSeries.toArray))
    }

    val df = session.createDataFrame(rows).toDF("id", "features")
    val newDf = VectorColumnScaler(transformType).fit(df).transform(df)

    for {
      rows <- newDf.select("scaledFeatures").rdd.collectAsync()
    } yield {
      newDf.unpersist()
      df.unpersist()

      rows.map(row =>
        row.getAs[Vector](0).toArray: Seq[Double]
      )
    }
  }
}
