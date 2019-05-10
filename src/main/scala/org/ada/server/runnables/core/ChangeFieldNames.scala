package org.ada.server.runnables.core

import javax.inject.Inject

import org.ada.server.dataaccess.StreamSpec
import org.ada.server.models.{DerivedDataSetSpec, RenameFieldsSpec}
import org.incal.core.runnables.InputFutureRunnable
import org.ada.server.services.DataSetService

import scala.reflect.runtime.universe.typeOf

class ChangeFieldNames @Inject() (dataSetService: DataSetService) extends InputFutureRunnable[ChangeFieldNamesSpec] {

  override def runAsFuture(input: ChangeFieldNamesSpec) =
    dataSetService.renameFields(
      RenameFieldsSpec(
        input.sourceDataSetId,
        input.oldFieldNames.zip(input.newFieldNames),
        input.resultDataSetSpec,
        input.streamSpec
      )
    )

  override def inputType = typeOf[ChangeFieldNamesSpec]
}

case class ChangeFieldNamesSpec(
  sourceDataSetId: String,
  oldFieldNames: Seq[String],
  newFieldNames: Seq[String],
  resultDataSetSpec: DerivedDataSetSpec,
  streamSpec: StreamSpec
)