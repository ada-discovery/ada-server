package org.ada.server.models

import reactivemongo.bson.BSONObjectID

case class Dictionary(
  _id: Option[BSONObjectID],
  dataSetId: String,
  fields: Seq[Field],
  categories: Seq[Category],
  filters: Seq[Filter],
  dataviews: Seq[DataView]
  //  classificationResults: Seq[ClassificationResult]
  //  parents : Seq[Dictionary],
)
