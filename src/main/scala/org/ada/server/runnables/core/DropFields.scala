package org.ada.server.runnables.core

import javax.inject.Inject

import org.ada.server.models.{DerivedDataSetSpec, DropFieldsSpec, RenameFieldsSpec}
import org.incal.core.runnables.InputFutureRunnable
import org.ada.server.services.DataSetService

import scala.reflect.runtime.universe.typeOf

class DropFields @Inject() (dataSetService: DataSetService) extends InputFutureRunnable[DropFieldsSpec] {

  override def runAsFuture(input: DropFieldsSpec) =
    dataSetService.dropFields(input)

  override def inputType = typeOf[DropFieldsSpec]
}