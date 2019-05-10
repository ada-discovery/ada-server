package org.ada.server.runnables.core

import javax.inject.Inject

import scala.reflect.runtime.universe.typeOf
import org.ada.server.models.DataSetLinkSpec
import org.incal.core.runnables.InputFutureRunnable
import org.ada.server.services.DataSetService

class LinkDataSets @Inject()(dataSetService: DataSetService) extends InputFutureRunnable[DataSetLinkSpec] {

  override def runAsFuture(input: DataSetLinkSpec) = dataSetService.linkDataSets(input)

  override def inputType = typeOf[DataSetLinkSpec]
}
