package org.ada.server.runnables.core

import javax.inject.Inject

import org.ada.server.models.StorageType
import org.incal.core.runnables.InputFutureRunnable
import org.ada.server.services.DataSetService

import scala.reflect.runtime.universe.typeOf

class CopyToNewStorage @Inject()(dataSetService: DataSetService) extends InputFutureRunnable[CopyToNewStorageSpec]{

  override def runAsFuture(input: CopyToNewStorageSpec) =
    dataSetService.copyToNewStorage(
      input.dataSetId,
      input.groupSize,
      input.parallelism,
      input.backpressureBufferSize,
      input.saveDeltaOnly,
      input.targetStorageType
    )

  override def inputType = typeOf[CopyToNewStorageSpec]
}

case class CopyToNewStorageSpec(
  dataSetId: String,
  groupSize: Int,
  parallelism: Int,
  backpressureBufferSize: Int,
  saveDeltaOnly: Boolean,
  targetStorageType: StorageType.Value
)