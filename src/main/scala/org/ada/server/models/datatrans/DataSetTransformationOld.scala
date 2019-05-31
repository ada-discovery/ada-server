package org.ada.server.models.datatrans

import org.ada.server.json.EnumFormat
import org.ada.server.models.{ScheduledTime, StorageType}
import org.incal.spark_ml.models.VectorScalerType
import play.api.libs.json.Json

trait DataSetTransformation2 {
  val resultDataSetSpec: ResultDataSetSpec
  def resultDataSetId = resultDataSetSpec.id
  def resultDataSetName = resultDataSetSpec.name
  def resultStorageType = resultDataSetSpec.storageType
}

case class SeriesProcessingSpec(
  fieldPath: String,
  processingType: SeriesProcessingType.Value,
  pastValuesCount: Int,
  addInitPaddingWithZeroes: Boolean = true
) {

  override def toString =
    if (pastValuesCount == 1)
      fieldPath + "_" + processingType.toString
    else
      fieldPath + "_" + processingType.toString + "-" + pastValuesCount.toString
}

case class DataSetSeriesProcessingSpec(
  sourceDataSetId: String,
  resultDataSetSpec: ResultDataSetSpec,
  seriesProcessingSpecs: Seq[SeriesProcessingSpec],
  preserveFieldNames: Seq[String],
  processingBatchSize: Option[Int],
  saveBatchSize: Option[Int]
) extends DataSetTransformation2


// TODO: This should be merged with DataSetSeriesProcessingSpec
case class DataSetSeriesTransformationSpec(
  sourceDataSetId: String,
  resultDataSetSpec: ResultDataSetSpec,
  seriesTransformationSpecs: Seq[SeriesTransformationSpec],
  preserveFieldNames: Seq[String],
  processingBatchSize: Option[Int],
  saveBatchSize: Option[Int]
) extends DataSetTransformation2

case class SeriesTransformationSpec(
  fieldPath: String,
  transformType: VectorScalerType.Value
) {
  override def toString =
    fieldPath + "_" + transformType.toString
}

case class DataSetLinkSpec(
  leftSourceDataSetId: String,
  rightSourceDataSetId: String,
  leftLinkFieldNames: Seq[String],
  rightLinkFieldNames: Seq[String],
  leftPreserveFieldNames: Traversable[String],
  rightPreserveFieldNames: Traversable[String],
  addDataSetIdToRightFieldNames: Boolean,
  resultDataSetSpec: ResultDataSetSpec,
  processingBatchSize: Option[Int] = None,
  saveBatchSize: Option[Int] = None,
  backpressureBufferSize: Option[Int] = None,
  parallelism: Option[Int] = None
) extends DataSetTransformation2 {
  def linkFieldNames = leftLinkFieldNames.zip(rightLinkFieldNames)
}

case class MultiDataSetLinkSpec(
  leftSourceDataSetId: String,
  rightSourceDataSetIds: Seq[String],
  leftLinkFieldNames: Seq[String],
  rightLinkFieldNames: Seq[Seq[String]],
  leftPreserveFieldNames: Traversable[String],
  rightPreserveFieldNames: Seq[Traversable[String]],
  addDataSetIdToRightFieldNames: Boolean,
  resultDataSetSpec: ResultDataSetSpec,
  processingBatchSize: Option[Int] = None,
  saveBatchSize: Option[Int] = None,
  backpressureBufferSize: Option[Int] = None,
  parallelism: Option[Int] = None
) extends DataSetTransformation2

case class SelfLinkSpec(
  dataSetId: String,
  keyFieldNames: Seq[String],
  valueFieldName: String,
  processingBatchSize: Option[Int],
  resultDataSetSpec: ResultDataSetSpec
) extends DataSetTransformation2

object SeriesProcessingType extends Enumeration {
  val Diff, RelativeDiff, Ratio, LogRatio, Min, Max, Mean = Value
}

object DataSetTransformation2 {
  implicit val storageTypeFormat = EnumFormat(StorageType)
  implicit val coreFormat = Json.format[ResultDataSetSpec]
  implicit val seriesProcessingTypeFormat = EnumFormat(SeriesProcessingType)
  implicit val seriesProcessingSpecFormat = Json.format[SeriesProcessingSpec]
  implicit val vectorTransformTypeFormat = EnumFormat(VectorScalerType)
  implicit val seriesTransformationSpecFormat = Json.format[SeriesTransformationSpec]
}