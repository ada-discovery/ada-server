package org.ada.server.runnables.core

import javax.inject.Inject
import org.ada.server.models.datatrans.SelfLinkSpec
import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.services.DataSetService

import scala.reflect.runtime.universe.typeOf
import scala.concurrent.ExecutionContext.Implicits.global

class SelfLink @Inject()(dataSetService: DataSetService) extends InputFutureRunnableExt[SelfLinkSpec] {

  override def runAsFuture(input: SelfLinkSpec) = dataSetService.selfLink(input)
}
