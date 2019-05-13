package org.ada.server.services.importers

import java.util.Calendar

import akka.actor.{ActorSystem, Cancellable}
import javax.inject.Inject
import org.ada.server.AdaException
import org.ada.server.dataaccess.RepoTypes.DataSetImportRepo
import org.ada.server.models.dataimport.ScheduledTime
import play.api.Logger
import reactivemongo.bson.BSONObjectID

import scala.collection.mutable.{Map => MMap}
import scala.concurrent.Await.result
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait DataSetImportScheduler {

  def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration)(
    importId: BSONObjectID
  ): Unit

  def schedule(
    scheduledTime: ScheduledTime)(
    importId: BSONObjectID
  ): Unit

  def cancel(importId: BSONObjectID): Unit
}

protected[services] class DataSetImportSchedulerImpl @Inject() (
    val system: ActorSystem,
    val dataSetImportRepo: DataSetImportRepo,
    dataSetImporterCentral: DataSetImporterCentral)(
    implicit ec: ExecutionContext
  ) extends DataSetImportScheduler {


  private val scheduledImports = MMap[BSONObjectID, Cancellable]()
  private val logger = Logger

  // schedule initial imports after five seconds
  system.scheduler.scheduleOnce(5 seconds) {
    init.recover {
      case e: Exception => logger.error(s"Initial data set import scheduling failed due to: ${e.getMessage}.")
    }
  }

  protected def init =
    dataSetImportRepo.find().map(_.map { importInfo =>
      if (importInfo.scheduled && importInfo.scheduledTime.isDefined)
        schedule(importInfo.scheduledTime.get)(importInfo._id.get)
    }).map(_ => ())

  override def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration)(
    importId: BSONObjectID
  ) {
    // cancel if already scheduled
    scheduledImports.get(importId).map(_.cancel)

    val newScheduledImport = system.scheduler.schedule(initialDelay, interval) {executeDataSetImport(importId)}
    scheduledImports.put(importId, newScheduledImport)
    logger.info(s"Import of data set #${importId} scheduled.")
  }

  override def schedule(
    scheduledTime: ScheduledTime)(
    importId: BSONObjectID
  ) =
    (schedule(_: FiniteDuration, _: FiniteDuration)_).tupled(toDelayAndInterval(scheduledTime))(importId)

  override def cancel(id: BSONObjectID) =
    scheduledImports.get(id).map { job =>
      job.cancel()
      logger.info(s"Import of data set #${id} canceled/descheduled.")
    }

  def executeDataSetImport(id: BSONObjectID): Future[Unit] = {
    for {
      dataSetImportOption <- dataSetImportRepo.get(id)
      _ <- dataSetImportOption.map(dataSetImporterCentral.apply).getOrElse(Future(()))
    } yield ()
  }.recover {
    case e: Exception => logger.error(s"Import of data set '${id}' failed due to: ${e.getMessage}.")
  }

  private def toDelayAndInterval(scheduledTime: ScheduledTime): (FiniteDuration, FiniteDuration) = {
    val hour = scheduledTime.hour
    val minute = scheduledTime.minute
    val second = scheduledTime.second

    val interval = if (hour.isDefined)
      1.day
    else if (minute.isDefined)
      1.hour
    else if (second.isDefined)
      1.minute
    else
      throw new AdaException("Hour, minute, or second have to be defined.")

    val now = Calendar.getInstance()
    val nextTime = Calendar.getInstance()
    if (hour.isDefined)
      nextTime.set(Calendar.HOUR_OF_DAY, hour.get)
    if (minute.isDefined)
      nextTime.set(Calendar.MINUTE, minute.get)
    if (second.isDefined)
      nextTime.set(Calendar.SECOND, second.get)

    val timeDiffMs = nextTime.getTimeInMillis - now.getTimeInMillis
    val initialDelayMs =
      if (timeDiffMs < 0) {
        val adjustedDelay = timeDiffMs - timeDiffMs * (timeDiffMs / interval.toMillis)
        if (adjustedDelay < 0)
          adjustedDelay + interval.toMillis
        else
          adjustedDelay
      } else
        timeDiffMs

    (initialDelayMs millis, interval)
  }
}