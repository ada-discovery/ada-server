package org.ada.server.runnables.core

import javax.inject.Inject
import org.ada.server.models.datatrans.DropFieldsTransformation
import org.ada.server.services.ServiceTypes.DataSetCentralTransformer
import org.incal.core.runnables.InputFutureRunnableExt

class DropFields @Inject() (centralTransformer: DataSetCentralTransformer) extends InputFutureRunnableExt[DropFieldsTransformation] {

  override def runAsFuture(input: DropFieldsTransformation) = centralTransformer(input)
}