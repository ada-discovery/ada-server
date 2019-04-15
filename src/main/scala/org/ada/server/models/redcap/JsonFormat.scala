package org.ada.server.models.redcap

import org.ada.server.json.EnumFormat
import play.api.libs.json.Json

object JsonFormat {
  implicit val fieldTypeFormat = EnumFormat(FieldType)
  implicit val metadataFormat = Json.format[Metadata]
  implicit val exportFieldFormat = Json.format[ExportField]
}
