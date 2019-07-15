package org.ada.server.models.redcap

import org.ada.server.json.EnumFormat
import play.api.libs.json.{Format, Json}
import play.api.libs.functional.syntax._
import play.api.libs.json._

object JsonFormat {
  implicit val fieldTypeFormat = EnumFormat(FieldType)
  implicit val metadataFormat = Json.format[Metadata]
  implicit val exportFieldFormat = Json.format[ExportField]
  implicit val eventFormat = Json.format[Event]

  implicit val responseFormat: Format[LockRecordResponse] = (
    (__ \ "record").format[String] and
    (__ \ "redcap_event_name").format[String] and
    (__ \ "instrument").format[String] and
    (__ \ "instance").format[Option[Int]](OptionalRobustIntFormat) and
    (__ \ "locked").format[String] and
    (__ \ "username").formatNullable[String] and
    (__ \ "timestamp").formatNullable[String]
  ) (LockRecordResponse.apply, unlift(LockRecordResponse.unapply))
}