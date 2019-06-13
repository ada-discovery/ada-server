package org.ada.server.models.redcap

case class LockRecordResponse(
  record: String,
  redcap_event_name: String,
  instrument: String,
  instance: Option[Int],
  locked: String,
  username: Option[String],
  timestamp: Option[String]
)