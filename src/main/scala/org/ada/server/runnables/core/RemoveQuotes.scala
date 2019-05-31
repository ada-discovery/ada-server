package org.ada.server.runnables.core

import javax.inject.Inject

import org.incal.core.runnables.InputFutureRunnableExt

import scala.reflect.runtime.universe.typeOf

class RemoveQuotes @Inject()(replaceString: ReplaceString) extends InputFutureRunnableExt[RemoveQuotesSpec] {

  override def runAsFuture(spec: RemoveQuotesSpec) =
    replaceString.runAsFuture(ReplaceStringSpec(spec.dataSetId, spec.fieldName, spec.batchSize, "\"", ""))
}

case class RemoveQuotesSpec(dataSetId: String, fieldName: String, batchSize: Int)