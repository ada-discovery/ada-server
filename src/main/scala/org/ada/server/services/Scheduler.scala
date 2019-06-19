package org.ada.server.services

import java.util.Calendar

import akka.actor.{ActorSystem, Cancellable}
import org.ada.server.AdaException
import org.ada.server.models.{Schedulable, ScheduledTime}
import org.incal.core.Identity
import org.incal.core.dataaccess.AsyncReadonlyRepo
import play.api.Logger

import scala.collection.mutable.{Map => MMap}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait Scheduler[IN <: Schedulable, ID] {

  def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration)(
    id: ID
  ): Unit

  def schedule(
    scheduledTime: ScheduledTime)(
    id: ID
  ): Unit

  def cancel(id: ID): Unit
}

protected[services] abstract class SchedulerImpl[IN <: Schedulable, ID] (
    execName: String)(
    implicit ec: ExecutionContext, identity: Identity[IN, ID]
  ) extends Scheduler[IN, ID] {

  protected val system: ActorSystem
  protected val repo: AsyncReadonlyRepo[IN, ID]
  protected val execCentral: LookupCentralExec[IN]

  private val scheduledExecs = MMap[ID, Cancellable]()
  private val logger = Logger

  // schedule initial execs after five seconds
  system.scheduler.scheduleOnce(5 seconds) {
    init.recover {
      case e: Exception => logger.error(s"Initial $execName scheduling failed due to: ${e.getMessage}.")
    }
  }

  protected def init =
    repo.find().map(_.map { importInfo =>
      if (importInfo.scheduled && importInfo.scheduledTime.isDefined) {
        val id = identity.of(importInfo).get // must exist
        schedule(importInfo.scheduledTime.get)(id)
      }
    }).map(_ => ())

  override def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration)(
    id: ID
  ) {
    // cancel if already scheduled
    scheduledExecs.get(id).map(_.cancel)

    val newScheduledExec = system.scheduler.schedule(initialDelay, interval)(exec(id))
    scheduledExecs.put(id, newScheduledExec)
    logger.info(s"${execName.capitalize} #${id.toString} scheduled.")
  }

  private def exec(id: ID): Future[Unit] = {
    for {
      dataSetImportOption <- repo.get(id)
      _ <- dataSetImportOption.map(execCentral(_)).getOrElse(Future(()))
    } yield ()
  }.recover {
    case e: Exception => logger.error(s"${execName.capitalize} '${id}' failed due to: ${e.getMessage}.")
  }

  override def schedule(
    scheduledTime: ScheduledTime)(
    id: ID
  ) =
    (schedule(_: FiniteDuration, _: FiniteDuration)_).tupled(toDelayAndInterval(scheduledTime))(id)

  override def cancel(id: ID) =
    scheduledExecs.get(id).map { job =>
      job.cancel()
      logger.info(s"${execName.capitalize} #${id.toString} canceled/descheduled.")
    }

  private def toDelayAndInterval(scheduledTime: ScheduledTime): (FiniteDuration, FiniteDuration) = {
    val weekDay = scheduledTime.weekDay
    val hour = scheduledTime.hour
    val minute = scheduledTime.minute
    val second = scheduledTime.second

    val interval =
      if (weekDay.isDefined)
        7.days
      else if (hour.isDefined)
        1.day
      else if (minute.isDefined)
        1.hour
      else if (second.isDefined)
        1.minute
      else
        throw new AdaException("Week day, hour, minute, or second have to be defined.")

    val now = Calendar.getInstance()

    val nextTime = Calendar.getInstance()

    if (weekDay.isDefined)
      nextTime.set(Calendar.DAY_OF_WEEK, weekDay.get.day)

    if (hour.isDefined)
      nextTime.set(Calendar.HOUR_OF_DAY, hour.get)

    if (minute.isDefined)
      nextTime.set(Calendar.MINUTE, minute.get)

    if (second.isDefined)
      nextTime.set(Calendar.SECOND, second.get)

    val timeDiffMs = nextTime.getTimeInMillis - now.getTimeInMillis

    val intervalMillis = interval.toMillis

    val initialDelayMs =
      if (timeDiffMs < 0) {
        val adjustedDelay = timeDiffMs - intervalMillis * (timeDiffMs / intervalMillis)
        if (adjustedDelay < 0) adjustedDelay + intervalMillis else adjustedDelay
      } else
        timeDiffMs

    (initialDelayMs millis, interval)
  }
}