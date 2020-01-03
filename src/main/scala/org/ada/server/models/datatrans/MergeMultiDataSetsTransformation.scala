package org.ada.server.models.datatrans

import java.util.Date

import org.ada.server.json.{HasFormat, OptionFormat}
import org.ada.server.models.ScheduledTime
import org.incal.core.dataaccess.StreamSpec
import org.ada.server.models.datatrans.DataSetTransformation._
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat
import play.api.libs.json.Json
import reactivemongo.bson.BSONObjectID

case class MergeMultiDataSetsTransformation(
  _id: Option[BSONObjectID] = None,

  val sourceDataSetIds: Seq[String],
  fieldNameMappings: Seq[Seq[Option[String]]],
  addSourceDataSetId: Boolean,

  resultDataSetSpec: ResultDataSetSpec,
  streamSpec: StreamSpec,
  scheduled: Boolean = false,
  scheduledTime: Option[ScheduledTime] = None,
  timeCreated: Date = new Date(),
  timeLastExecuted: Option[Date] = None
) extends DataSetTransformation {

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

object MergeMultiDataSetsTransformation extends HasFormat[MergeMultiDataSetsTransformation] {
  implicit val optionStringFormat = new OptionFormat[String]

  val format = Json.format[MergeMultiDataSetsTransformation]
}