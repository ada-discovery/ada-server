package org.ada.server.models.datatrans

trait CoreLinkTwoDataSetsTransformation {

  this: DataSetTransformation =>

  val leftSourceDataSetId: String
  val rightSourceDataSetId: String
  val linkFieldNames: Seq[(String, String)]
  val leftFieldNamesToKeep: Traversable[String]
  val rightFieldNamesToKeep: Traversable[String]
  val addDataSetIdToRightFieldNames: Boolean
}
