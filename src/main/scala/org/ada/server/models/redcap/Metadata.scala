package org.ada.server.models.redcap

case class Metadata(
  field_name: String,
  form_name: String,
  section_header: String,
  field_type: FieldType.Value,
  field_label: String,
  select_choices_or_calculations: String,
  field_note: String,
  //    text_validation_type_or_show_slider: String,
  text_validation_min: String,
  text_validation_max: String,
  identifier: String,
  branching_logic: String,
  required_field: String,
  custom_alignment: String,
  question_number: String,
  matrix_group_name: String,
  matrix_ranking: String,
  field_annotation: String
)

object FieldType extends Enumeration {
  val radio, calc, text, checkbox, descriptive, yesno, dropdown, notes, file, slider, truefalse, sql = Value
}