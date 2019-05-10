package org.ada.server.runnables.core

import javax.inject.Inject

import org.incal.core.runnables.InputFutureRunnable
import scala.reflect.runtime.universe.typeOf

class ReplaceCommaWithDot @Inject()(replaceString: ReplaceString) extends InputFutureRunnable[ReplaceCommaWithDotSpec] {

  override def runAsFuture(spec: ReplaceCommaWithDotSpec) =
    replaceString.runAsFuture(ReplaceStringSpec(spec.dataSetId, spec.fieldName, spec.batchSize, ",", "."))

  override def inputType = typeOf[ReplaceCommaWithDotSpec]
}

case class ReplaceCommaWithDotSpec(dataSetId: String, fieldName: String, batchSize: Int)