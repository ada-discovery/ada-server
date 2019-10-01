package org.ada.server.models.dataimport

import org.ada.server.models._
import org.ada.server.dataaccess.BSONObjectIdentity
import org.ada.server.json.{EnumFormat, HasFormat, RuntimeClassFormat, SubTypeFormat}
import reactivemongo.bson.BSONObjectID
import java.util.Date

import org.ada.server.services.StaticLookupCentralImpl
import play.api.libs.json._

trait DataSetImport extends Schedulable {
  val _id: Option[BSONObjectID]
  val timeCreated: Date
  val timeLastExecuted: Option[Date]

  val dataSpaceName: String
  val dataSetId: String
  val dataSetName: String
  val setting: Option[DataSetSetting]

  val dataView: Option[DataView]

  def copyCore(
    _id: Option[BSONObjectID],
    timeCreated: Date,
    timeLastExecuted: Option[Date],
    scheduled: Boolean,
    scheduledTime: Option[ScheduledTime]
  ): DataSetImport
}

object DataSetImport {
  implicit val weekDayFormat = EnumFormat(WeekDay)
  implicit val scheduleTimeFormat = Json.format[ScheduledTime]
  implicit val dataSetSettingFormat = DataSetFormattersAndIds.dataSetSettingFormat
  implicit val dataViewFormat = DataView.dataViewFormat

  private val subFormats = new StaticLookupCentralImpl[HasFormat[DataSetImport]](
    getClass.getPackage.getName, true
  ).apply.map(x => RuntimeClassFormat(x.format, x.runtimeClass))

  implicit val dataSetImportFormat: Format[DataSetImport] = new SubTypeFormat[DataSetImport](subFormats)

  implicit object DataSetImportIdentity extends BSONObjectIdentity[DataSetImport] {
    def of(entity: DataSetImport): Option[BSONObjectID] = entity._id

    protected def set(entity: DataSetImport, id: Option[BSONObjectID]) =
      entity.copyCore(id, entity.timeCreated, entity.timeLastExecuted, entity.scheduled, entity.scheduledTime)
  }
}