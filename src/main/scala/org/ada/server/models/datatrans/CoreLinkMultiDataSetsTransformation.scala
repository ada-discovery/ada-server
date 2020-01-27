package org.ada.server.models.datatrans

trait CoreLinkMultiDataSetsTransformation {
  this: DataSetTransformation =>

  val linkedDataSetSpecs: Seq[LinkedDataSetSpec]
  val addDataSetIdToRightFieldNames: Boolean
}

case class LinkedDataSetSpec(
  dataSetId: String,
  linkFieldNames: Seq[String],
  explicitFieldNamesToKeep: Traversable[String] = Nil
)