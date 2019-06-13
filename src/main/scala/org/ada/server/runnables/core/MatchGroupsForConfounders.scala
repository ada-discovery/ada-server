package org.ada.server.runnables.core

import javax.inject.Inject
import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.models.datatrans.MatchGroupsWithConfoundersTransformation
import org.ada.server.services.ServiceTypes.DataSetCentralTransformer

class MatchGroupsWithConfounders @Inject() (centralTransformer: DataSetCentralTransformer) extends InputFutureRunnableExt[MatchGroupsWithConfoundersTransformation] {

  override def runAsFuture(input: MatchGroupsWithConfoundersTransformation)=
    centralTransformer(input)
}