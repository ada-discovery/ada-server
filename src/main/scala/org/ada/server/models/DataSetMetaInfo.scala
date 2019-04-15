package org.ada.server.models

import java.util.Date

import reactivemongo.bson.BSONObjectID

case class DataSetMetaInfo(
  _id: Option[BSONObjectID],
  id: String,
  name: String,
  sortOrder: Int,
  hide: Boolean,
  dataSpaceId: BSONObjectID,
  timeCreated: Date = new Date(),
  sourceDataSetId: Option[BSONObjectID] = None
)
