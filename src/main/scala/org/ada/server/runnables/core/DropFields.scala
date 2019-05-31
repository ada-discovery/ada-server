package org.ada.server.runnables.core

import javax.inject.Inject
import org.ada.server.models.datatrans.DropFieldsTransformation
import org.incal.core.runnables.{InputFutureRunnable, InputFutureRunnableExt}
import org.ada.server.services.transformers.DropFieldsTransformer

class DropFields @Inject() (transformer: DropFieldsTransformer) extends InputFutureRunnableExt[DropFieldsTransformation] {

  override def runAsFuture(input: DropFieldsTransformation) = transformer.runAsFuture(input)
}