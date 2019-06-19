package org.ada.server.models

import java.util.Calendar

trait Schedulable {
  val scheduled: Boolean
  val scheduledTime: Option[ScheduledTime]
}

case class ScheduledTime(
  weekDay: Option[WeekDay.Value],
  hour: Option[Int],
  minute: Option[Int],
  second: Option[Int]
)

object WeekDay extends Enumeration {

  case class Val(day: Int) extends super.Val
  implicit def valueToWeekDayVal(x: Value): Val = x.asInstanceOf[Val]

  val Monday = Val(Calendar.MONDAY)
  val Tuesday = Val(Calendar.TUESDAY)
  val Wednesday = Val(Calendar.WEDNESDAY)
  val Thursday = Val(Calendar.THURSDAY)
  val Friday = Val(Calendar.FRIDAY)
  val Saturday = Val(Calendar.SATURDAY)
  val Sunday = Val(Calendar.SUNDAY)
}