package org.ada.server.runnables.core

import javax.inject.Inject

import org.ada.server.dataaccess.DataSetMetaInfoRepoFactory
import org.ada.server.dataaccess.RepoTypes.DataSpaceMetaInfoRepo
import org.ada.server.AdaException
import org.ada.server.dataaccess.dataset.DataSetAccessorFactory
import reactivemongo.bson.BSONObjectID
import org.incal.core.runnables.InputFutureRunnableExt
import org.incal.core.util.seqFutures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.universe.typeOf

class MoveDataSets @Inject() (
    dataSetMetaInfoRepoFactory: DataSetMetaInfoRepoFactory,
    dataSpaceMetaInfoRepo: DataSpaceMetaInfoRepo,
    dsaf: DataSetAccessorFactory
  ) extends InputFutureRunnableExt[MoveDataSetsSpec] {

  override def runAsFuture(input: MoveDataSetsSpec) = {
    val dataSetIds =
      (input.suffixFrom, input.suffixTo).zipped.headOption.map { case (from, to) =>
        (from to to).map(input.dataSetId + _)
      }.getOrElse(
        Seq(input.dataSetId)
      )

    for {
      _ <- seqFutures(dataSetIds) { move(_, input.newDataSpaceId) }
    } yield
      ()
  }

  private def move(
    dataSetId: String,
    newDataSpaceId: BSONObjectID
  ): Future[Unit] = {
    val dsa = dsaf(dataSetId).getOrElse(
      throw new AdaException(s"Data set $dataSetId not found.")
    )

    for {
      // get a meta info associated with the data set
      metaInfo <- dsa.metaInfo

      // new meta info
      newMetaInfo = metaInfo.copy(dataSpaceId = newDataSpaceId)

      // delete meta info at the old data space
      _ <- {
        val repo = dataSetMetaInfoRepoFactory(metaInfo.dataSpaceId)
        repo.delete(metaInfo._id.get)
      }

      // old data space meta info
      oldDataSpaceMetaInfo <- dataSpaceMetaInfoRepo.get(metaInfo.dataSpaceId)

      // remove from the old space and update
      _ <- {
        oldDataSpaceMetaInfo.map { oldDataSpaceMetaInfo =>
          val filteredMetaInfos = oldDataSpaceMetaInfo.dataSetMetaInfos.filterNot(_._id == metaInfo._id)
          dataSpaceMetaInfoRepo.update(oldDataSpaceMetaInfo.copy(dataSetMetaInfos = filteredMetaInfos))
        }.getOrElse(
          Future(())
        )
      }

      // save it to a new one
      _ <- {
        val repo = dataSetMetaInfoRepoFactory(newDataSpaceId)
        repo.save(newMetaInfo)
      }

      // new data space meta info
      newDataSpaceMetaInfo <- dataSpaceMetaInfoRepo.get(newDataSpaceId)

      // add to the new space and update
      _ <- {
        newDataSpaceMetaInfo.map { newDataSpaceMetaInfo =>
          val newMetaInfos = newDataSpaceMetaInfo.dataSetMetaInfos ++ Seq(newMetaInfo)
          dataSpaceMetaInfoRepo.update(newDataSpaceMetaInfo.copy(dataSetMetaInfos = newMetaInfos))
        }.getOrElse(
          Future(())
        )
      }

      // update a meta info
      _ <- dsa.updateMetaInfo(newMetaInfo)
    } yield
      ()
  }
}

case class MoveDataSetsSpec(newDataSpaceId: BSONObjectID, dataSetId: String, suffixFrom: Option[Int], suffixTo: Option[Int])