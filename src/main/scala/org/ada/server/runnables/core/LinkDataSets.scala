package org.ada.server.runnables.core

import javax.inject.Inject
import org.ada.server.models.datatrans.DataSetLinkSpec
import scala.reflect.runtime.universe.typeOf
import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.services.DataSetService

class LinkDataSets @Inject()(dataSetService: DataSetService) extends InputFutureRunnableExt[DataSetLinkSpec] {

  override def runAsFuture(input: DataSetLinkSpec) = dataSetService.linkDataSets(input)
}
