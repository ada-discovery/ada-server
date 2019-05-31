package org.ada.server.runnables.core

import javax.inject.Inject

import org.ada.server.field.{FieldTypeHelper, FieldTypeInferrerFactory}
import org.ada.server.models.StorageType
import org.ada.server.models.DataSetSetting
import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.services.DataSetService

import scala.reflect.runtime.universe.typeOf

class InferNewDataSet @Inject()(dataSetService: DataSetService) extends InputFutureRunnableExt[InferNewDataSetSpec] {

  override def runAsFuture(spec: InferNewDataSetSpec) = {
    val fieldTypeInferrerFactory = FieldTypeInferrerFactory(
      FieldTypeHelper.fieldTypeFactory(booleanIncludeNumbers = spec.booleanIncludeNumbers),
      spec.maxEnumValuesCount,
      spec.minAvgValuesPerEnum,
      FieldTypeHelper.arrayDelimiter
    )

    val dataSetSetting = new DataSetSetting(spec.newDataSetId, spec.storageType)

    dataSetService.translateDataAndDictionaryOptimal(
      spec.originalDataSetId,
      spec.newDataSetId,
      spec.newDataSetName,
      Some(dataSetSetting),
      None,
      spec.saveBatchSize,
      spec.inferenceGroupSize,
      spec.inferenceGroupsInParallel,
      Some(fieldTypeInferrerFactory.applyJson)
    )
  }
}

case class InferNewDataSetSpec(
  originalDataSetId: String,
  newDataSetId: String,
  newDataSetName: String,
  storageType: StorageType.Value,
  saveBatchSize: Option[Int],
  inferenceGroupSize: Option[Int],
  inferenceGroupsInParallel: Option[Int],
  maxEnumValuesCount: Int,
  minAvgValuesPerEnum: Double,
  booleanIncludeNumbers: Boolean
)