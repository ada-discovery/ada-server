package org.ada.server.models

import org.ada.server.dataaccess.StreamSpec
import org.ada.server.models.StorageType
import org.ada.server.json.EnumFormat
import org.incal.spark_ml.models.VectorScalerType
import play.api.libs.json.Json

trait DataSetTransformation {
  val resultDataSetSpec: DerivedDataSetSpec
  def resultDataSetId = resultDataSetSpec.id
  def resultDataSetName = resultDataSetSpec.name
  def resultStorageType = resultDataSetSpec.storageType
}

case class DerivedDataSetSpec(
  id: String,
  name: String,
  storageType: StorageType.Value
)

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
  resultDataSetSpec: DerivedDataSetSpec,
  seriesProcessingSpecs: Seq[SeriesProcessingSpec],
  preserveFieldNames: Seq[String],
  processingBatchSize: Option[Int],
  saveBatchSize: Option[Int]
) extends DataSetTransformation

// TODO: This should be merged with DataSetSeriesProcessingSpec
case class DataSetSeriesTransformationSpec(
  sourceDataSetId: String,
  resultDataSetSpec: DerivedDataSetSpec,
  seriesTransformationSpecs: Seq[SeriesTransformationSpec],
  preserveFieldNames: Seq[String],
  processingBatchSize: Option[Int],
  saveBatchSize: Option[Int]
) extends DataSetTransformation

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
  resultDataSetSpec: DerivedDataSetSpec,
  processingBatchSize: Option[Int] = None,
  saveBatchSize: Option[Int] = None,
  backpressureBufferSize: Option[Int] = None,
  parallelism: Option[Int] = None
) extends DataSetTransformation {
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
  resultDataSetSpec: DerivedDataSetSpec,
  processingBatchSize: Option[Int] = None,
  saveBatchSize: Option[Int] = None,
  backpressureBufferSize: Option[Int] = None,
  parallelism: Option[Int] = None
) extends DataSetTransformation

case class SelfLinkSpec(
  dataSetId: String,
  keyFieldNames: Seq[String],
  valueFieldName: String,
  processingBatchSize: Option[Int],
  resultDataSetSpec: DerivedDataSetSpec
) extends DataSetTransformation

case class DropFieldsSpec(
  sourceDataSetId: String,
  fieldNamesToKeep: Traversable[String],
  fieldNamesToDrop: Traversable[String],
  resultDataSetSpec: DerivedDataSetSpec,
  streamSpec: StreamSpec
) extends DataSetTransformation

case class RenameFieldsSpec(
  sourceDataSetId: String,
  fieldOldNewNames: Traversable[(String, String)],
  resultDataSetSpec: DerivedDataSetSpec,
  streamSpec: StreamSpec
) extends DataSetTransformation

object SeriesProcessingType extends Enumeration {
  val Diff, RelativeDiff, Ratio, LogRatio, Min, Max, Mean = Value
}

object DataSetTransformation {
  implicit val storageTypeFormat = EnumFormat(StorageType)
  implicit val coreFormat = Json.format[DerivedDataSetSpec]
  implicit val seriesProcessingTypeFormat = EnumFormat(SeriesProcessingType)
  implicit val seriesProcessingSpecFormat = Json.format[SeriesProcessingSpec]
  implicit val vectorTransformTypeFormat = EnumFormat(VectorScalerType)
  implicit val seriesTransformationSpecFormat = Json.format[SeriesTransformationSpec]
}