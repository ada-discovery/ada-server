package org.ada.server.models

trait Schedulable {
  val scheduled: Boolean
  val scheduledTime: Option[ScheduledTime]
}

case class ScheduledTime(
  hour: Option[Int],
  minute: Option[Int],
  second: Option[Int]
)