package org.ada.server.models.ml.classification

import org.ada.server.dataaccess.BSONObjectIdentity
import org.ada.server.json._
import org.incal.spark_ml.models.VectorScalerType
import org.incal.spark_ml.models.classification._
import org.incal.spark_ml.models.result.{BinaryClassificationCurves, ClassificationMetricStats, ClassificationResult, MetricStatsValues, StandardClassificationResult, TemporalClassificationResult}
import org.incal.spark_ml.models.setting._
import org.ada.server.models.ml.classification.Classifier.eitherFormat
import org.ada.server.models.ml.ReservoirSpec.reservoirSpecFormat
import play.api.libs.json.{Json, _}
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats._

object ClassificationResult {

  implicit val classificationResultFormat: Format[ClassificationResult] = {
    implicit val vectorScalerTypeFormat = EnumFormat(VectorScalerType)
    implicit val evalMetricFormat = EnumFormat(ClassificationEvalMetric)
    createClassificationResultFormat(vectorScalerTypeFormat, evalMetricFormat)
  }

  implicit val standardClassificationResultFormat: Format[StandardClassificationResult] = {
    implicit val vectorScalerTypeFormat = EnumFormat(VectorScalerType)
    implicit val evalMetricFormat = EnumFormat(ClassificationEvalMetric)
    createStandardClassificationResultFormat(vectorScalerTypeFormat, evalMetricFormat)
  }

  implicit val temporalClassificationResultFormat: Format[TemporalClassificationResult] = {
    implicit val vectorScalerTypeFormat = EnumFormat(VectorScalerType)
    implicit val evalMetricFormat = EnumFormat(ClassificationEvalMetric)
    createTemporalClassificationResultFormat(vectorScalerTypeFormat, evalMetricFormat)
  }

  implicit val standardClassificationRunSpecFormat: Format[ClassificationRunSpec] = {
    implicit val vectorScalerTypeFormat = EnumFormat(VectorScalerType)
    implicit val evalMetricFormat = EnumFormat(ClassificationEvalMetric)
    createStandardClassificationRunSpecFormat(vectorScalerTypeFormat, evalMetricFormat)
  }

  implicit val temporalClassificationRunSpecFormat: Format[TemporalClassificationRunSpec] = {
    implicit val vectorScalerTypeFormat = EnumFormat(VectorScalerType)
    implicit val evalMetricFormat = EnumFormat(ClassificationEvalMetric)
    createTemporalClassificationRunSpecFormat(vectorScalerTypeFormat, evalMetricFormat)
  }

  implicit object ClassificationResultIdentity extends BSONObjectIdentity[ClassificationResult] {
    def of(entity: ClassificationResult): Option[BSONObjectID] = entity._id

    protected def set(entity: ClassificationResult, id: Option[BSONObjectID]) =
      entity match {
        case x: StandardClassificationResult => x.copy(_id = id)
        case x: TemporalClassificationResult => x.copy(_id = id)
      }
  }

  // helper functions

  def createClassificationResultFormat(
    implicit vectorScalerTypeFormat: Format[VectorScalerType.Value],
    evalMetricFormat: Format[ClassificationEvalMetric.Value]
  ): Format[ClassificationResult] = {
    implicit val standardFormat = createStandardClassificationResultFormat(vectorScalerTypeFormat, evalMetricFormat)
    implicit val temporalFormat = createTemporalClassificationResultFormat(vectorScalerTypeFormat, evalMetricFormat)

    new SubTypeFormat[ClassificationResult](
      Seq(
        RuntimeClassFormat(standardFormat),
        RuntimeClassFormat(temporalFormat)
      )
    )
  }

  def createStandardClassificationResultFormat(
    implicit vectorScalerTypeFormat: Format[VectorScalerType.Value],
    evalMetricFormat: Format[ClassificationEvalMetric.Value]
  ) = {
    implicit val classificationRunSpecFormat =
      createStandardClassificationRunSpecFormat(vectorScalerTypeFormat, evalMetricFormat)

    implicit val classificationMetricStatsValuesFormat = Json.format[MetricStatsValues]
    implicit val classificationMetricStatsFormat = Json.format[ClassificationMetricStats]
    implicit val doubleTupleFormat = TupleFormat[Double, Double]
    implicit val binaryClassificationCurvesFormat = Json.format[BinaryClassificationCurves]

    new FlattenFormat(Json.format[StandardClassificationResult], "-", Set("_id", "filterId", "replicationFilterId", "mlModelId"))
  }

  def createTemporalClassificationResultFormat(
    implicit vectorScalerTypeFormat: Format[VectorScalerType.Value],
    evalMetricFormat: Format[ClassificationEvalMetric.Value]
  ) = {
    implicit val classificationRunSpecFormat =
      createTemporalClassificationRunSpecFormat(vectorScalerTypeFormat, evalMetricFormat)

    implicit val classificationMetricStatsValuesFormat = Json.format[MetricStatsValues]
    implicit val classificationMetricStatsFormat = Json.format[ClassificationMetricStats]
    implicit val doubleTupleFormat = TupleFormat[Double, Double]
    implicit val binaryClassificationCurvesFormat = Json.format[BinaryClassificationCurves]

    new FlattenFormat(Json.format[TemporalClassificationResult], "-", Set("_id", "filterId", "replicationFilterId", "mlModelId"))
  }

  private def createStandardClassificationRunSpecFormat(
    implicit vectorScalerTypeFormat: Format[VectorScalerType.Value],
    evalMetricFormat: Format[ClassificationEvalMetric.Value]
  ) = {
    implicit val tupleFormat = TupleFormat[String, Double]
    implicit val learningSettingFormat = Json.format[ClassificationLearningSetting]
    implicit val ioSpecFormat = Json.format[IOSpec]

    Json.format[ClassificationRunSpec]
  }

  private def createTemporalClassificationRunSpecFormat(
    implicit vectorScalerTypeFormat: Format[VectorScalerType.Value],
    evalMetricFormat: Format[ClassificationEvalMetric.Value]
  ) = {
    implicit val tupleFormat = TupleFormat[String, Double]
    implicit val learningSettingFormat = Json.format[ClassificationLearningSetting]
    implicit val intEitherFormat = eitherFormat[Int]
    implicit val temporalLearningSettingFormat = Json.format[TemporalClassificationLearningSetting]

    implicit val ioSpecFormat = Json.format[TemporalGroupIOSpec]
    Json.format[TemporalClassificationRunSpec]
  }
}