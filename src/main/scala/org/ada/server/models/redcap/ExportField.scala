package org.ada.server.models.redcap

case class ExportField(
  original_field_name: String,
  choice_value: String,
  export_field_name: String
)