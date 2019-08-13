package org.ada.server.services.ml

import javax.inject.{Inject, Singleton}
import com.google.inject.ImplementedBy
import org.ada.server.models.DataSetFormattersAndIds.JsObjectIdentity
import org.ada.server.models.{Field, FieldTypeId, FieldTypeSpec}
import org.ada.server.models.ml.IOJsonTimeSeriesSpec
import org.ada.server.services.SparkApp
import org.ada.server.services.StatsService
import org.apache.spark.ml.feature._
import org.apache.spark.ml._
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructType, _}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.sql.functions._
import play.api.libs.json.{JsObject, Json}
import play.api.Configuration
import org.incal.spark_ml.transformers._
import org.incal.spark_ml.models.VectorScalerType
import org.incal.spark_ml._
import org.incal.spark_ml.models.classification.{ClassificationEvalMetric, Classifier}
import org.incal.spark_ml.models.clustering.Clustering
import org.incal.spark_ml.models.regression.{RegressionEvalMetric, Regressor}
import org.incal.spark_ml.models.result.{ClassificationResultsHolder, RegressionResultsHolder}
import org.incal.spark_ml.models.setting.{ClassificationLearningSetting, RegressionLearningSetting, TemporalClassificationLearningSetting, TemporalRegressionLearningSetting}

import scala.concurrent.{Await, Future}

@ImplementedBy(classOf[MachineLearningServiceImpl])
trait MachineLearningService {

  // Classification

  def classifyStatic(
    data: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    outputFieldName: String,
    mlModel: Classifier,
    setting: ClassificationLearningSetting = ClassificationLearningSetting(),
    replicationData: Traversable[JsObject] = Nil
  ): Future[ClassificationResultsHolder]

  def classifyTemporalSeries(
    data: JsObject,
    ioSpec: IOJsonTimeSeriesSpec,
    mlModel: Classifier,
    setting: TemporalClassificationLearningSetting,
    replicationData: Option[JsObject] = None
  ): Future[ClassificationResultsHolder]

  def classifyRowTemporalSeries(
    data: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    inputFieldNames: Seq[String],
    outputFieldName: String,
    orderFieldName: String,
    orderedValues: Seq[Any],
    groupIdFieldName: Option[String],
    mlModel: Classifier,
    setting: TemporalClassificationLearningSetting,
    replicationData: Traversable[JsObject] = Nil
  ): Future[ClassificationResultsHolder]

  // Regression

  def regressStatic(
    data: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    outputFieldName: String,
    mlModel: Regressor,
    setting: RegressionLearningSetting = RegressionLearningSetting(),
    replicationData: Traversable[JsObject] = Nil
  ): Future[RegressionResultsHolder]

  def regressTemporalSeries(
    data: JsObject,
    ioSpec: IOJsonTimeSeriesSpec,
    mlModel: Regressor,
    setting: TemporalRegressionLearningSetting,
    replicationData: Option[JsObject] = None
  ): Future[RegressionResultsHolder]

  def regressRowTemporalSeries(
    data: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    inputFieldNames: Seq[String],
    outputFieldName: String,
    orderFieldName: String,
    orderedValues: Seq[Any],
    groupIdFieldName: Option[String],
    mlModel: Regressor,
    setting: TemporalRegressionLearningSetting,
    replicationData: Traversable[JsObject] = Nil
  ): Future[RegressionResultsHolder]

  // Clustering

  def clusterDf(
    dataFrame: DataFrame,
    idColumnName: String,
    mlModel: Clustering,
    featuresNormalizationType: Option[VectorScalerType.Value],
    pcaDim: Option[Int] = None
  ): Traversable[(String, Int)]

  def clusterBasic(
    data: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    mlModel: Clustering,
    featuresNormalizationType: Option[VectorScalerType.Value],
    pcaDim: Option[Int] = None
  ): Traversable[(String, Int)]

  def clusterAndGetPCA12(
    data: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    mlModel: Clustering,
    featuresNormalizationType: Option[VectorScalerType.Value],
    pcaDim: Option[Int] = None
  ): (Traversable[(String, Int)], Traversable[(String, (Double, Double))])

  def pcaComponents(
    k: Int)(
    df: DataFrame
  ): DataFrame

  // AFTSurvivalRegression
  // IsotonicRegression
}

@Singleton
private class MachineLearningServiceImpl @Inject() (
    sparkApp: SparkApp,
    configuration: Configuration,
    statsService: StatsService,
    val rcStatesWindowFactory: RCStatesWindowFactory
  ) extends MachineLearningService with SparkMLService {

  override val setting = SparkMLServiceSetting(
    repetitionParallelism = configuration.getInt("ml.repetition_parallelism"),
    binaryClassifierInputName = configuration.getString("ml.binary_classifier.input"),
    useConsecutiveOrderForDL = configuration.getBoolean("ml.dl_use_consecutive_order_transformers"),
    debugMode = configuration.getBoolean("ml.debug_mode").getOrElse(false)
  )

  private val session = sparkApp.session
  private implicit val sqlContext = sparkApp.sqlContext

  override def classifyStatic(
    data: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    outputFieldName: String,
    mlModel: Classifier,
    setting: ClassificationLearningSetting,
    replicationData: Traversable[JsObject]
  ): Future[ClassificationResultsHolder] = {

    // create training-test and replication data

    // aux function to create a data frame
    def crateDataFrame(jsons: Traversable[JsObject]) = {
      val df = FeaturesDataFrameFactory(session, jsons, fields, Some(outputFieldName))
      BooleanLabelIndexer(Some("labelString")).transform(df)
    }

    // create a training/test data frame with all the features
    val df = crateDataFrame(data)

    // create a replication data frame with all the features
    val replicationDf = if (replicationData.nonEmpty) Some(crateDataFrame(replicationData)) else None

    // run classification with the newly created data frames
    classify(df, mlModel, setting, replicationDf)
  }

  override def classifyTemporalSeries(
    data: JsObject,
    ioSpec: IOJsonTimeSeriesSpec,
    mlModel: Classifier,
    setting: TemporalClassificationLearningSetting,
    replicationData: Option[JsObject]
  ): Future[ClassificationResultsHolder] = {
    // create training-test and replication data

    // aux function to create a data frame
    def crateDataFrame(json: JsObject) = {
      val df = FeaturesDataFrameFactory.applySeries(session)(json, ioSpec, seriesOrderCol)
      BooleanLabelIndexer(Some("labelString")).transform(df)
    }

    // create a training/test data frame with all the features
    val df = crateDataFrame(data)

    // create a replication data frame with all the features
    val replicationDf = replicationData.map(crateDataFrame)

    // run time-series classification with the newly created data frames
    classifyTimeSeries(df, mlModel, setting, None, replicationDf)
  }

  override def classifyRowTemporalSeries(
    data: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    inputFieldNames: Seq[String],
    outputFieldName: String,
    orderFieldName: String,
    orderedValues: Seq[Any],
    groupIdFieldName: Option[String],
    mlModel: Classifier,
    setting: TemporalClassificationLearningSetting,
    replicationData: Traversable[JsObject]
  ): Future[ClassificationResultsHolder] = {

    // create training-test and replication data

    // aux function to create a series (ordered) data frame
    val createSeriesDf = createSeriesDataFrame(
      fields,
      inputFieldNames,
      outputFieldName,
      orderFieldName,
      orderedValues,
      groupIdFieldName
    )

    // aux function to create a data frame
    val crateDataFrame = (jsons: Traversable[JsObject]) => {
      val seriesDf = createSeriesDf(jsons)
      BooleanLabelIndexer(Some("labelString")).transform(seriesDf)
    }

    // create a training/test data frame with all the features
    val df = crateDataFrame(data)

    df.show(truncate = false)

    // create a replication data frame with all the features
    val replicationDf = if (replicationData.nonEmpty) Some(crateDataFrame(replicationData)) else None

    // run time-series classification with the newly created data frames
    classifyTimeSeries(df, mlModel, setting, groupIdFieldName, replicationDf)
  }

  private def mapValuesUDF(map: Map[Any, Int]) = udf { value: Any =>
    if (map.isEmpty) {
      value.asInstanceOf[Int]
    } else {
      map.get(value).getOrElse(
        throw new IllegalStateException(s"The map $map does not contain a value $value.")
      )
    }
  }

  override def regressStatic(
    data: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    outputFieldName: String,
    mlModel: Regressor,
    setting: RegressionLearningSetting,
    replicationData: Traversable[JsObject]
  ): Future[RegressionResultsHolder] = {
    // create training-test and replication data

    // aux function to create a data frame
    def crateDataFrame(jsons: Traversable[JsObject]) =
      FeaturesDataFrameFactory(session, jsons, fields, Some(outputFieldName))

    // create a training/test data frame with all the features
    val df = crateDataFrame(data)

    // create a replication data frame with all the features
    val replicationDf = if (replicationData.nonEmpty) Some(crateDataFrame(replicationData)) else None

    // run regression with the newly created data frames
    regress(df, mlModel, setting, replicationDf)
  }

  override def regressTemporalSeries(
    data: JsObject,
    ioSpec: IOJsonTimeSeriesSpec,
    mlModel: Regressor,
    setting: TemporalRegressionLearningSetting,
    replicationData: Option[JsObject]
  ): Future[RegressionResultsHolder] = {
    // create training-test and replication data

    // aux function to create a data frame
    def crateDataFrame(json: JsObject) = FeaturesDataFrameFactory.applySeries(session)(json, ioSpec, seriesOrderCol)

    // create a training/test data frame with all the features
    val df = crateDataFrame(data)

    // create a replication data frame with all the features
    val replicationDf = replicationData.map(crateDataFrame)

    // run time-series regression with the newly created data frames
    regressTimeSeries(df, mlModel, setting, None, replicationDf)
  }

  override def regressRowTemporalSeries(
    data: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    inputFieldNames: Seq[String],
    outputFieldName: String,
    orderFieldName: String,
    orderedValues: Seq[Any],
    groupIdFieldName: Option[String],
    mlModel: Regressor,
    setting: TemporalRegressionLearningSetting,
    replicationData: Traversable[JsObject]
  ): Future[RegressionResultsHolder] = {

    // create training-test and replication data

    // aux function to create a data frame
    val crateDataFrame = createSeriesDataFrame(
      fields,
      inputFieldNames,
      outputFieldName,
      orderFieldName,
      orderedValues,
      groupIdFieldName
    )

    // create a training/test data frame with all the features
    val df = crateDataFrame(data)

    df.show(truncate = false)

    // create a replication data frame with all the features
    val replicationDf = if (replicationData.nonEmpty) Some(crateDataFrame(replicationData)) else None

    // run time-series regression with the newly created data frames
    regressTimeSeries(df, mlModel, setting, groupIdFieldName, replicationDf)
  }

  // aux function to create a data frame
  protected def createSeriesDataFrame(
    fields: Seq[(String, FieldTypeSpec)],
    inputFieldNames: Traversable[String],
    outputFieldName: String,
    orderFieldName: String,
    orderedValues: Seq[Any],
    groupIdFieldName: Option[String] = None
  ): Traversable[JsObject] => DataFrame = {

    // mapping between an order value and index
    val orderValueIndexFun = mapValuesUDF(orderedValues.zipWithIndex.toMap)

    // transformer to filter groups with an insufficient count
    val filterGroups = groupIdFieldName.map(FilterOrderedGroupsWithCount(_, orderedValues.size))

    (jsons: Traversable[JsObject]) =>
      val df = FeaturesDataFrameFactory(session, jsons, fields)
      val featuresDf = SparkUtil.prepFeaturesDataFrame(inputFieldNames.toSet, Some(outputFieldName))(df)

      // if ordered values are defined use their position as index, otherwise we assume the given values are integers and can be used directly as index
      val seriesDf =
        if (orderedValues.nonEmpty) {
          featuresDf.withColumn(seriesOrderCol, orderValueIndexFun(featuresDf(orderFieldName))).drop(orderFieldName)
        } else {
          featuresDf.withColumn(seriesOrderCol, featuresDf(orderFieldName).cast(IntegerType)).drop(orderFieldName)
        }

      // filter groups
      filterGroups.map(_.transform(seriesDf)).getOrElse(seriesDf)
  }

  override def clusterBasic(
    data: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    mlModel: Clustering,
    featuresNormalizationType: Option[VectorScalerType.Value],
    pcaDim: Option[Int]
  ): Traversable[(String, Int)] = {
    val (_, idClusters) = clusterAux(data, fields, mlModel, featuresNormalizationType, pcaDim)
    idClusters
  }

  override def clusterAndGetPCA12(
    data: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    mlModel: Clustering,
    featuresNormalizationType: Option[VectorScalerType.Value],
    pcaDim: Option[Int]
  ): (Traversable[(String, Int)], Traversable[(String, (Double, Double))]) = {
    val (df, idClusters) = clusterAux(data, fields, mlModel, featuresNormalizationType, pcaDim)

    // reduce the dimensionality if needed
    val pca12Df = InPlacePCA(2).fit(df).transform(df)

    import sparkApp.session.implicits._

    val idPca12Values = pca12Df.select(JsObjectIdentity.name, "features").map { r =>
      val id = r(0).asInstanceOf[String]
      val values = r(1).asInstanceOf[DenseVector].values
      (id, (values(0), values(1)))
    }.collect

    (idClusters, idPca12Values)
  }

  override def clusterDf(
    dataFrame: DataFrame,
    idColumnName: String,
    mlModel: Clustering,
    featuresNormalizationType: Option[VectorScalerType.Value],
    pcaDim: Option[Int] = None
  ): Traversable[(String, Int)] = {
    val featureNames = dataFrame.columns.filterNot(_.equals(idColumnName))
    val featureDf = dataFrame.transform(
      SparkUtil.prepFeaturesDataFrame(featureNames.toSet, None)
    )

    val resultsDf = cluster(featureDf, mlModel, featuresNormalizationType, pcaDim)

    idClusterPairs(resultsDf, idColumnName)
  }

  private def clusterAux(
    data: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    mlModel: Clustering,
    featuresNormalizationType: Option[VectorScalerType.Value],
    pcaDim: Option[Int]
  ): (DataFrame, Traversable[(String, Int)]) = {
    // prepare a data frame for learning
    val featureFieldNames = fields.map(_._1)
    val fieldsWithId = fields ++ Seq((JsObjectIdentity.name, FieldTypeSpec(FieldTypeId.String)))
    val df = FeaturesDataFrameFactory(session, data, fieldsWithId, featureFieldNames)

    val resultsDf = cluster(df, mlModel, featuresNormalizationType, pcaDim)

    (resultsDf, idClusterPairs(resultsDf, JsObjectIdentity.name))
  }

  private def idClusterPairs(
    resultsDf: DataFrame,
    idColumnName: String
  ): Traversable[(String, Int)] = {
    implicit val encoder = Encoders.tuple(Encoders.STRING, Encoders.scalaInt)

    resultsDf.select(idColumnName, "cluster").map { r =>
      val id = r(0).asInstanceOf[String]
      val clazz = r(1).asInstanceOf[Int]
      (id, clazz + 1)
    }.collect
  }

  override def pcaComponents(
    k: Int)(
    df: DataFrame
  ): DataFrame =
    InPlacePCA(k).fit(df).transform(df)
}