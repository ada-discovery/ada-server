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

object ScheduledTime {

  def fillZeroes(scheduledTime: ScheduledTime): ScheduledTime = {
    def value(int: Option[Int]) = Some(int.getOrElse(0))

    if (scheduledTime.weekDay.isDefined) {
      scheduledTime.copy(hour = value(scheduledTime.hour), minute = value(scheduledTime.minute), second = value(scheduledTime.second))
    } else if (scheduledTime.hour.isDefined) {
      scheduledTime.copy(minute = value(scheduledTime.minute), second = value(scheduledTime.second))
    } else if (scheduledTime.minute.isDefined) {
      scheduledTime.copy(second = value(scheduledTime.second))
    } else
      scheduledTime
  }
}

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