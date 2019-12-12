package org.ada.server.runnables.core

import javax.inject.Inject
import org.incal.core.dataaccess.StreamSpec
import org.ada.server.models.datatrans.{RenameFieldsTransformation, ResultDataSetSpec}
import org.ada.server.services.ServiceTypes.DataSetCentralTransformer
import org.incal.core.runnables.{InputFutureRunnable, InputFutureRunnableExt}

class ChangeFieldNames @Inject() (centralTransformer: DataSetCentralTransformer) extends InputFutureRunnableExt[ChangeFieldNamesSpec] {

  override def runAsFuture(input: ChangeFieldNamesSpec) =
    centralTransformer(
      RenameFieldsTransformation(
        sourceDataSetId = input.sourceDataSetId,
        fieldOldNewNames = input.oldFieldNames.zip(input.newFieldNames),
        resultDataSetSpec = input.resultDataSetSpec,
        streamSpec = input.streamSpec
      )
    )
}

case class ChangeFieldNamesSpec(
  sourceDataSetId: String,
  oldFieldNames: Seq[String],
  newFieldNames: Seq[String],
  resultDataSetSpec: ResultDataSetSpec,
  streamSpec: StreamSpec
)