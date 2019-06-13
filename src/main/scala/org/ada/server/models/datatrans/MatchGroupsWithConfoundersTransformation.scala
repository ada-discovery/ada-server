package org.ada.server.models.datatrans

import java.util.Date

import org.ada.server.dataaccess.StreamSpec
import org.ada.server.models.ScheduledTime
import reactivemongo.bson.BSONObjectID

case class MatchGroupsWithConfoundersTransformation(
  _id: Option[BSONObjectID] = None,
  sourceDataSetId: String,
  filterId: Option[BSONObjectID] = None,
  targetGroupFieldName: String,
  confoundingFieldNames: Seq[String],
  numericDistTolerance: Double,
  targetGroupDisplayStringRatios: Seq[(String, Option[Int])] = Nil,
  resultDataSetSpec: ResultDataSetSpec,
  streamSpec: StreamSpec,
  scheduled: Boolean = false,
  scheduledTime: Option[ScheduledTime] = None,
  timeCreated: Date = new Date(),
  timeLastExecuted: Option[Date] = None
) extends DataSetTransformation {
  override val sourceDataSetIds = Seq(sourceDataSetId)
}