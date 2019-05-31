package org.ada.server.models.datatrans

import java.util.Date

import org.ada.server.dataaccess.{BSONObjectIdentity, StreamSpec}
import org.ada.server.json.{EnumFormat, ManifestedFormat, SubTypeFormat, TupleFormat}
import org.ada.server.models.{Schedulable, ScheduledTime, StorageType}
import play.api.libs.json.{Format, Json}
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats._

trait DataSetTransformation extends Schedulable {
  val _id: Option[BSONObjectID]
  val timeCreated: Date
  val timeLastExecuted: Option[Date]

  val sourceDataSetIds: Seq[String]
  val resultDataSetSpec: ResultDataSetSpec
  val streamSpec: StreamSpec

  def resultDataSetId = resultDataSetSpec.id
  def resultDataSetName = resultDataSetSpec.name
  def resultStorageType = resultDataSetSpec.storageType
}

case class ResultDataSetSpec(
  id: String,
  name: String,
  storageType: StorageType.Value
)

object DataSetTransformation {
  implicit val scheduleTimeFormat = Json.format[ScheduledTime]
  implicit val storageTypeFormat = EnumFormat(StorageType)
  implicit val resultDataSetSpecFormat = Json.format[ResultDataSetSpec]
  implicit val streamSpecFormat = Json.format[StreamSpec]
  implicit val tupleFormat = TupleFormat[String, String]
  implicit val tupleFormat2 = TupleFormat[String, Seq[(String, String)]]

  implicit val dataSetTransformationFormat: Format[DataSetTransformation] = new SubTypeFormat[DataSetTransformation](
    Seq(
      ManifestedFormat(Json.format[CopyDataSetTransformation]),
      ManifestedFormat(Json.format[DropFieldsTransformation]),
      ManifestedFormat(Json.format[RenameFieldsTransformation]),
      ManifestedFormat(Json.format[ChangeFieldEnumsTransformation])
    )
  )

  implicit object DataSetTransformationIdentity extends BSONObjectIdentity[DataSetTransformation] {
    def of(entity: DataSetTransformation): Option[BSONObjectID] = entity._id

    protected def set(entity: DataSetTransformation, id: Option[BSONObjectID]) =
      entity match {
        case x: CopyDataSetTransformation => x.copy(_id = id)
        case x: DropFieldsTransformation => x.copy(_id = id)
        case x: RenameFieldsTransformation => x.copy(_id = id)
        case x: ChangeFieldEnumsTransformation => x.copy(_id = id)
      }
  }

  implicit class DataSetTransformationExt(val dataSetImport: DataSetTransformation) extends AnyVal {

    def copyWithTimestamps(timeCreated: Date, timeLastExecuted: Option[Date]) =
      dataSetImport match {
        case x: CopyDataSetTransformation => x.copy(timeCreated = timeCreated, timeLastExecuted = timeLastExecuted)
        case x: DropFieldsTransformation => x.copy(timeCreated = timeCreated, timeLastExecuted = timeLastExecuted)
        case x: RenameFieldsTransformation => x.copy(timeCreated = timeCreated, timeLastExecuted = timeLastExecuted)
        case x: ChangeFieldEnumsTransformation => x.copy(timeCreated = timeCreated, timeLastExecuted = timeLastExecuted)
      }
  }
}