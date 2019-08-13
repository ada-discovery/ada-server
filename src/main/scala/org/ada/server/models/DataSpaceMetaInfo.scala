package org.ada.server.models

import java.util.Date
import reactivemongo.bson.BSONObjectID
import scala.collection.mutable.Buffer
import scala.collection.mutable.ListBuffer

case class DataSpaceMetaInfo(
  _id: Option[BSONObjectID],
  name: String,
  sortOrder: Int,
  timeCreated: Date = new Date(),
  dataSetMetaInfos: Seq[DataSetMetaInfo] = Nil,
  parentId: Option[BSONObjectID] = None,
  var children: Buffer[DataSpaceMetaInfo] = ListBuffer[DataSpaceMetaInfo]()
)