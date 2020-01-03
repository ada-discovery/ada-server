package org.ada.server.services.transformers

import javax.inject.Inject
import org.ada.server.models.datatrans.{MergeFullyMultiDataSetsTransformation, MergeMultiDataSetsTransformation}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private class MergeFullyMultiDataSetsTransformer @Inject()(multiTransformer: MergeMultiDataSetsTransformer) extends AbstractDataSetTransformer[MergeFullyMultiDataSetsTransformation] {

  override def runAsFuture(
    spec: MergeFullyMultiDataSetsTransformation
  ) = {
    val dsafs = spec.sourceDataSetIds.map(dsaSafe)
    val fieldRepos = dsafs.map(_.fieldRepo)

    for {
      // collect all the field names in parallel
      allFieldNameSets <- Future.sequence(
        fieldRepos.map(
          _.find().map(_.map(_.name).toSet)
        )
      )

      // merge all the field names
      allFieldNames = allFieldNameSets.flatten.toSet

      // create field name mappings
      fieldNameMappings = allFieldNames.map(fieldName =>
        allFieldNameSets.map(set =>
          if (set.contains(fieldName)) Some(fieldName) else None
        )
      ).toSeq

      // call a general merge-data-sets transformation with given field mappings
      _ <- multiTransformer.runAsFuture(
        MergeMultiDataSetsTransformation(
          None,
          spec.sourceDataSetIds,
          fieldNameMappings,
          spec.addSourceDataSetId,
          spec.resultDataSetSpec,
          spec.streamSpec
        )
      )
    } yield
      ()
  }

  protected def execInternal(
    spec: MergeFullyMultiDataSetsTransformation
  ) = ??? // not called
}