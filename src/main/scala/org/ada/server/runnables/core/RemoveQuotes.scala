package org.ada.server.runnables.core

import javax.inject.Inject

import org.incal.core.runnables.InputFutureRunnable

import scala.reflect.runtime.universe.typeOf

class RemoveQuotes @Inject()(replaceString: ReplaceString) extends InputFutureRunnable[RemoveQuotesSpec] {

  override def runAsFuture(spec: RemoveQuotesSpec) =
    replaceString.runAsFuture(ReplaceStringSpec(spec.dataSetId, spec.fieldName, spec.batchSize, "\"", ""))

  override def inputType = typeOf[RemoveQuotesSpec]
}

case class RemoveQuotesSpec(dataSetId: String, fieldName: String, batchSize: Int)