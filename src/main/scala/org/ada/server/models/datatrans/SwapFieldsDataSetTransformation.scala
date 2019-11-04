package org.ada.server.models.datatrans

import java.util.Date

import org.ada.server.dataaccess.StreamSpec
import org.ada.server.json.HasFormat
import org.ada.server.models.{Field, ScheduledTime}
import org.ada.server.models.DataSetFormattersAndIds.fieldFormat
import play.api.libs.json.Json
import reactivemongo.bson.BSONObjectID
import org.ada.server.models.datatrans.DataSetTransformation._
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat

case class SwapFieldsDataSetTransformation(
  _id: Option[BSONObjectID] = None,

  sourceDataSetId: String,
  resultDataSetSpec: ResultDataSetSpec,
  newFields: Seq[Field],

  streamSpec: StreamSpec,
  scheduled: Boolean = false,
  scheduledTime: Option[ScheduledTime] = None,
  timeCreated: Date = new Date(),
  timeLastExecuted: Option[Date] = None
) extends DataSetTransformation {

  override val sourceDataSetIds = Seq(sourceDataSetId)

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

object SwapFieldsDataSetTransformation extends HasFormat[SwapFieldsDataSetTransformation] {
  val format = Json.format[SwapFieldsDataSetTransformation]
}
