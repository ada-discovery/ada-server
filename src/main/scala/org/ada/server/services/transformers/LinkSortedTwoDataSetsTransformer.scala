package org.ada.server.services.transformers

import javax.inject.Inject
import org.ada.server.models.datatrans._

private class LinkSortedTwoDataSetsTransformer @Inject()(multiTransformer: LinkSortedMultiDataSetsTransformer) extends AbstractDataSetTransformer[LinkSortedTwoDataSetsTransformation] {

  // just delegates to LinkSortedMultiDataSetsTransformer
  override def runAsFuture(
    spec: LinkSortedTwoDataSetsTransformation
  ) = {
    multiTransformer.runAsFuture(
      LinkSortedMultiDataSetsTransformation(
        linkedDataSetSpecs = Seq(
          LinkedDataSetSpec(spec.leftSourceDataSetId, spec.linkFieldNames.map(_._1), spec.leftFieldNamesToKeep),
          LinkedDataSetSpec(spec.rightSourceDataSetId, spec.linkFieldNames.map(_._2), spec.rightFieldNamesToKeep)
        ),
        addDataSetIdToRightFieldNames = spec.addDataSetIdToRightFieldNames,
        resultDataSetSpec = spec.resultDataSetSpec,
        streamSpec = spec.streamSpec
      )
    )
  }

  protected def execInternal(
    spec: LinkSortedTwoDataSetsTransformation
  ) = ??? // not called
}