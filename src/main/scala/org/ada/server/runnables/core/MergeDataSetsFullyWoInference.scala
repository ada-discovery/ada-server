package org.ada.server.runnables.core

import javax.inject.Inject
import org.ada.server.dataaccess.StreamSpec
import org.ada.server.models.datatrans.ResultDataSetSpec
import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.services.DataSetService

import scala.reflect.runtime.universe.typeOf

class MergeDataSetsFullyWoInference @Inject() (dataSetService: DataSetService) extends InputFutureRunnableExt[MergeDataSetsFullyWoInferenceSpec] {

  override def runAsFuture(input: MergeDataSetsFullyWoInferenceSpec) =
    dataSetService.mergeDataSetsFullyWoInference(
      input.sourceDataSetIds,
      input.addSourceDataSetId,
      input.resultDataSetSpec,
      input.streamSpec
    )
}

case class MergeDataSetsFullyWoInferenceSpec(
  sourceDataSetIds: Seq[String],
  addSourceDataSetId: Boolean,
  resultDataSetSpec: ResultDataSetSpec,
  streamSpec: StreamSpec
)
