package org.ada.server.services.transformers

import javax.inject.Inject
import org.ada.server.models.datatrans.{LinkMultiDataSetsTransformation, LinkTwoDataSetsTransformation, LinkedDataSetSpec}

private class LinkTwoDataSetsTransformer @Inject()(multiTransformer: LinkMultiDataSetsTransformer) extends AbstractDataSetTransformer[LinkTwoDataSetsTransformation] {

  // just delegates to LinkMultiDataSetsTransformer
  override def runAsFuture(
    spec: LinkTwoDataSetsTransformation
  ) = {
    multiTransformer.runAsFuture(
      LinkMultiDataSetsTransformation(
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
    spec: LinkTwoDataSetsTransformation
  ) = ??? // not called
}