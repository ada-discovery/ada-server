package org.ada.server.models.datatrans

import java.util.Date

import org.ada.server.json.HasFormat
import org.ada.server.models.ScheduledTime
import org.incal.core.dataaccess.StreamSpec
import play.api.libs.json.Json
import reactivemongo.bson.BSONObjectID

case class LinkSortedTwoDataSetsTransformation(
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
) extends DataSetTransformation with CoreLinkTwoDataSetsTransformation {

  override val sourceDataSetIds = Seq(leftSourceDataSetId, rightSourceDataSetId)

  override def copyCore(
    __id: Option[BSONObjectID],
    _timeCreated: Date,
    _timeLastExecuted: Option[Date],
    _scheduled: Boolean,
    _scheduledTime: Option[ScheduledTime]
  ) = copy(
    _id = __id,
    timeCreated = _timeCreated,
    timeLastExecuted = _timeLastExecuted,
    scheduled = _scheduled,
    scheduledTime = _scheduledTime
  )
}

object LinkSortedTwoDataSetsTransformation extends HasFormat[LinkSortedTwoDataSetsTransformation] {
  val format = Json.format[LinkSortedTwoDataSetsTransformation]
}
