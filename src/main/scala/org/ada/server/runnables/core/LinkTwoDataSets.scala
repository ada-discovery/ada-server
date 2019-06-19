package org.ada.server.runnables.core

import javax.inject.Inject
import org.ada.server.models.datatrans.LinkTwoDataSetsTransformation

import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.services.ServiceTypes.DataSetCentralTransformer

class LinkTwoDataSets @Inject()(centralTransformer: DataSetCentralTransformer) extends InputFutureRunnableExt[LinkTwoDataSetsTransformation] {

  override def runAsFuture(input: LinkTwoDataSetsTransformation) =
    centralTransformer(input)
}
