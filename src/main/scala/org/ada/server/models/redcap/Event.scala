package org.ada.server.models.redcap

case class Event(
  unique_event_name: String,
  event_name: String,
  custom_event_label: Option[String],
  offset_max: Int,
  offset_min: Int,
  day_offset: Int,
  arm_num: Int
)
