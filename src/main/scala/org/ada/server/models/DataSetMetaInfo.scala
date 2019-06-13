package org.ada.server.models

import java.util.Date

import reactivemongo.bson.BSONObjectID

case class DataSetMetaInfo(
  _id: Option[BSONObjectID] = None,
  id: String,
  name: String,
  description: Option[String] = None,
  sortOrder: Int = 0,
  hide: Boolean = false,
  dataSpaceId: BSONObjectID,
  timeCreated: Date = new Date(),
  sourceDataSetId: Option[BSONObjectID] = None
)
