package org.ada.server.runnables.core

import javax.inject.Inject

import org.ada.server.field.FieldTypeHelper
import org.ada.server.models.StorageType
import org.ada.server.models.DataSetSetting
import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.services.DataSetService

import scala.reflect.runtime.universe.typeOf

class TranslateNewDataSet @Inject()(dataSetService: DataSetService) extends InputFutureRunnableExt[TranslateNewDataSetSpec] {

  override def runAsFuture(spec: TranslateNewDataSetSpec) = {

    val dataSetSetting = new DataSetSetting(spec.newDataSetId, spec.storageType)

    dataSetService.translateData(
      spec.originalDataSetId,
      spec.newDataSetId,
      spec.newDataSetName,
      Some(dataSetSetting),
      None,
      spec.saveBatchSize
    )
  }
}

case class TranslateNewDataSetSpec(
  originalDataSetId: String,
  newDataSetId: String,
  newDataSetName: String,
  storageType: StorageType.Value,
  saveBatchSize: Option[Int]
)