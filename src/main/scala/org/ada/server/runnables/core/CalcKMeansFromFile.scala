package org.ada.server.runnables.core

import java.nio.file.Paths

import com.google.inject.Inject
import org.incal.spark_ml.models.clustering.{BisectingKMeans, Clustering, KMeans}
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.hadoop.fs._
import play.api.Logger
import org.incal.core.runnables.{InputRunnable, InputRunnableExt}
import org.incal.core.util.GroupMapList3
import org.incal.core.{PlotSetting, PlotlyPlotter}
import org.ada.server.services.SparkApp
import org.ada.server.services.ml.MachineLearningService

import collection.JavaConverters._

import scala.util.Random
import org.incal.core.util.{listFiles, writeStringAsStream}

class CalcKMeansFromFile @Inject()(
    val sparkApp: SparkApp,
    val machineLearningService: MachineLearningService
  ) extends InputRunnableExt[CalcKMeansFromFileSpec] with CalcKMeansHelper {

  def run(input: CalcKMeansFromFileSpec) = {
    val model =
      if (input.useBisecting)
        BisectingKMeans(None, k = input.k, maxIteration = input.maxIterations)
      else
        KMeans(None, k = input.k, maxIteration = input.maxIterations)

    val exportPlotFileName = if (input.exportPlot) Some(input.exportFileName + ".png") else None
    calcKMeansAux(input.inputFileName, input.delimiter, input.exportFileName, exportPlotFileName, model)
  }
}

class CalcKMeansFromFolder @Inject()(
    val sparkApp: SparkApp,
    val machineLearningService: MachineLearningService
  ) extends InputRunnableExt[CalcKMeansFromFolderSpec] with CalcKMeansHelper {

  def run(input: CalcKMeansFromFolderSpec) = {
    val model =
      if (input.useBisecting)
        BisectingKMeans(None, k = input.k, maxIteration = input.maxIterations)
      else
        KMeans(None, k = input.k, maxIteration = input.maxIterations)

    val modelPrefix = if (input.useBisecting) "bisKMeans" else "kMeans"
    val iterPart = input.maxIterations.map(iter => "_iter_" + iter).getOrElse("")

    val inputFileNames = listFiles(input.inputFolderName).map(_.getName).filter(_.endsWith(input.extension))

    inputFileNames.map { inputFileName =>
      logger.info(s"Executing k-means with k=${input.k} for the file '$inputFileName'.")
      val exportFileBaseName = inputFileName.substring(0, inputFileName.size - (input.extension.size + 1)) + s"-${modelPrefix}_${input.k}${iterPart}"
      val exportFileName = input.exportFolderName + "/" + exportFileBaseName + ".csv"
      val exportPlotFileName = if (input.exportPlot) Some(input.exportFolderName + "/" + exportFileBaseName + ".html") else None

      calcKMeansAux(input.inputFolderName + "/" + inputFileName, input.delimiter, exportFileName, exportPlotFileName, model)
    }
  }
}

case class CalcKMeansFromFileSpec(
  k: Int,
  maxIterations: Option[Int],
  useBisecting: Boolean,
  inputFileName: String,
  delimiter: Option[String],
  exportFileName: String,
  exportPlot: Boolean
)

case class CalcKMeansFromFolderSpec(
  k: Int,
  maxIterations: Option[Int],
  useBisecting: Boolean,
  inputFolderName: String,
  extension: String,
  delimiter: Option[String],
  exportFolderName: String,
  exportPlot: Boolean
)

trait CalcKMeansHelper {

  val sparkApp: SparkApp
  val machineLearningService: MachineLearningService

  protected val logger = Logger
  private val session = sparkApp.session
  private val fs = FileSystem.get(sparkApp.sc.hadoopConfiguration)

  private val clusterClassColumnName = "clazz"
  private val defaultDelimiter = ","

  protected def calcKMeansAux(
    inputFileName: String,
    delimiter: Option[String],
    exportFileName: String,
    exportPlotFileName: Option[String],
    model: Clustering
  ) = {
    val unescapedDelimiter = StringEscapeUtils.unescapeJava(delimiter.getOrElse(defaultDelimiter))

    val df = session.read.format("csv").option("header", "true").option("delimiter", unescapedDelimiter).option("inferSchema", "true").load(inputFileName)

    val idColumnName = df.columns(0)

    var idClusters = (0 to 3).foldLeft(Nil: Traversable[(String, Int)]) { case (idClusters, trial) =>
      if (idClusters.isEmpty || idClusters.forall(_._2 == 1)) {
        val newModel = if (trial > 0) {
          logger.error(s"A single cluster obtained! Repeating the clustering (trial $trial).")
          setCurrentTimeAsSeed(model)
        } else
          model

        machineLearningService.clusterDf(df, idColumnName, newModel, None, None)
      } else
        idClusters
    }

    val rows = idClusters.toList.map(Row.fromTuple).asJava

    val schema = StructType(Seq(
      StructField(idColumnName, StringType, false),
      StructField(clusterClassColumnName, IntegerType, false)
    ))

    val idClustersDf = session.createDataFrame(rows, schema)
    val finalDf = df.join(idClustersDf, Seq(idColumnName))

    // export results to as a csv
    val exportDir = createRandomTempDirName
    finalDf.write.option("header", "true").csv(exportDir)

    val file = fs.globStatus(new Path(exportDir + "/part*"))(0).getPath().getName()

    fs.rename(new Path(exportDir + "/" + file), new Path(exportFileName))
    fs.delete(new Path(exportDir), true)

    // export as a plot
    if (exportPlotFileName.isDefined) {
      val featureColumnNames = df.columns.filterNot(_.equals(idColumnName)).toSeq
      val finalDfRows = finalDf.select(clusterClassColumnName, featureColumnNames: _*).collect()

      val labelValues = finalDfRows.map { row =>
        (row.getInt(0).toString, row.getDouble(1), row.getDouble(2))
      }.toSeq

      val labelValuesGrouped = labelValues.toGroupMap

      val values = labelValuesGrouped.map(_._2)
      val labels = labelValuesGrouped.map(_._1).toSeq

      PlotlyPlotter.plotScatter(values, PlotSetting(title = Some("k-Means"), captions = labels), exportPlotFileName.get)
    }
  }

  private def setCurrentTimeAsSeed(
    model: Clustering
  ): Clustering =
    model match {
      case kMeans: KMeans => kMeans.copy(seed = Some(new java.util.Date().getTime))
      case bisectingKMeans: BisectingKMeans => bisectingKMeans.copy(seed = Some(new java.util.Date().getTime))
      case _ => model
    }

  private def createRandomTempDirName = {
    val tmpDir = Paths.get(System.getProperty("java.io.tmpdir"))
    val name = tmpDir.getFileSystem.getPath(Random.nextLong().toString)
    if (name.getParent != null) throw new IllegalArgumentException("Invalid prefix or suffix")
    tmpDir.resolve(name).toString
  }
}