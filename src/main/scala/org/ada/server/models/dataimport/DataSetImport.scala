package org.ada.server.models.dataimport

import org.ada.server.models._
import org.ada.server.dataaccess.BSONObjectIdentity
import org.ada.server.json.{EnumFormat, ManifestedFormat, SubTypeFormat}
import reactivemongo.bson.BSONObjectID
import java.util.Date

import reactivemongo.play.json.BSONFormats._
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
}

object DataSetImport {
  implicit val weekDayFormat = EnumFormat(WeekDay)
  implicit val scheduleTimeFormat = Json.format[ScheduledTime]
  implicit val dataSetSettingFormat = DataSetFormattersAndIds.dataSetSettingFormat
  implicit val dataViewFormat = DataView.dataViewFormat

  implicit val dataSetImportFormat: Format[DataSetImport] = new SubTypeFormat[DataSetImport](
    Seq(
      ManifestedFormat(Json.format[CsvDataSetImport]),
      ManifestedFormat(Json.format[JsonDataSetImport]),
      ManifestedFormat(Json.format[SynapseDataSetImport]),
      ManifestedFormat(Json.format[TranSmartDataSetImport]),
      ManifestedFormat(Json.format[RedCapDataSetImport]),
      ManifestedFormat(Json.format[EGaitDataSetImport])
    )
  )

  implicit object DataSetImportIdentity extends BSONObjectIdentity[DataSetImport] {
    def of(entity: DataSetImport): Option[BSONObjectID] = entity._id

    protected def set(entity: DataSetImport, id: Option[BSONObjectID]) =
      entity match {
        case x: CsvDataSetImport => x.copy(_id = id)
        case x: JsonDataSetImport => x.copy(_id = id)
        case x: SynapseDataSetImport => x.copy(_id = id)
        case x: TranSmartDataSetImport => x.copy(_id = id)
        case x: RedCapDataSetImport => x.copy(_id = id)
        case x: EGaitDataSetImport => x.copy(_id = id)
      }
  }

  implicit class DataSetImportExt(val dataSetImport: DataSetImport) extends AnyVal {

    def copyWithTimestamps(timeCreated: Date, timeLastExecuted: Option[Date]): DataSetImport =
      dataSetImport match {
        case x: CsvDataSetImport => x.copy(timeCreated = timeCreated, timeLastExecuted = timeLastExecuted)
        case x: JsonDataSetImport => x.copy(timeCreated = timeCreated, timeLastExecuted = timeLastExecuted)
        case x: SynapseDataSetImport => x.copy(timeCreated = timeCreated, timeLastExecuted = timeLastExecuted)
        case x: TranSmartDataSetImport => x.copy(timeCreated = timeCreated, timeLastExecuted = timeLastExecuted)
        case x: RedCapDataSetImport => x.copy(timeCreated = timeCreated, timeLastExecuted = timeLastExecuted)
        case x: EGaitDataSetImport => x.copy(timeCreated = timeCreated, timeLastExecuted = timeLastExecuted)
      }
  }
}