package org.ada.server.models.datatrans

import java.util.Date

import org.ada.server.dataaccess.StreamSpec
import org.ada.server.models.ScheduledTime
import reactivemongo.bson.BSONObjectID

case class ChangeFieldEnumsTransformation(
  _id: Option[BSONObjectID] = None,

  sourceDataSetId: String,
  fieldNameOldNewEnums: Seq[(String, String, String)],

  scheduled: Boolean = false,
  scheduledTime: Option[ScheduledTime] = None,
  timeCreated: Date = new Date(),
  timeLastExecuted: Option[Date] = None
) extends DataSetMetaTransformation {
  override val sourceDataSetIds = Seq(sourceDataSetId)
}