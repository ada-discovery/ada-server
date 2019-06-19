package org.ada.server.models.datatrans

import java.util.Date

import org.ada.server.dataaccess.StreamSpec
import org.ada.server.models.ScheduledTime
import reactivemongo.bson.BSONObjectID

case class LinkTwoDataSetsTransformation(
  _id: Option[BSONObjectID] = None,

  leftSourceDataSetId: String,
  rightSourceDataSetId: String,
  linkFieldNames: Seq[(String, String)],
  leftFieldNamesToKeep: Traversable[String] = Nil,
  rightFieldNamesToKeep: Traversable[String] = Nil,
  addDataSetIdToRightFieldNames: Boolean = true,

  resultDataSetSpec: ResultDataSetSpec,
  streamSpec: StreamSpec,
  scheduled: Boolean = false,
  scheduledTime: Option[ScheduledTime] = None,
  timeCreated: Date = new Date(),
  timeLastExecuted: Option[Date] = None
) extends DataSetTransformation {
  override val sourceDataSetIds = Seq(leftSourceDataSetId, rightSourceDataSetId)
}