package org.ada.server.runnables.core

import javax.inject.Inject
import org.ada.server.field.FieldTypeHelper
import org.ada.server.field.inference.FieldTypeInferrerFactory
import org.ada.server.models.StorageType
import org.ada.server.models.DataSetSetting
import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.services.DataSetService

import scala.reflect.runtime.universe.typeOf

class InferNewDataSet @Inject()(dataSetService: DataSetService) extends InputFutureRunnableExt[InferNewDataSetSpec] {

  override def runAsFuture(spec: InferNewDataSetSpec) = {

    val dataSetSetting = new DataSetSetting(spec.newDataSetId, spec.storageType)

    val fieldTypeInferrerFactory = new FieldTypeInferrerFactory(
      FieldTypeHelper.fieldTypeFactory(),
      spec.maxEnumValuesCount,
      spec.minAvgValuesPerEnum,
      FieldTypeHelper.arrayDelimiter
    )

    dataSetService.inferData(
      spec.originalDataSetId,
      spec.newDataSetId,
      spec.newDataSetName,
      Some(dataSetSetting),
      None,
      spec.saveBatchSize,
      spec.inferenceGroupSize,
      spec.inferenceGroupsInParallel,
      Some(fieldTypeInferrerFactory.ofJson)
    )
  }
}

case class InferNewDataSetSpec(
  originalDataSetId: String,
  newDataSetId: String,
  newDataSetName: String,
  storageType: StorageType.Value,
  maxEnumValuesCount: Int,
  minAvgValuesPerEnum: Double,
  saveBatchSize: Option[Int],
  inferenceGroupSize: Option[Int],
  inferenceGroupsInParallel: Option[Int]
)