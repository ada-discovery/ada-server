package org.ada.server.runnables.core

import javax.inject.Inject

import org.ada.server.dataaccess.RepoTypes.DataSpaceMetaInfoRepo
import play.api.Logger
import reactivemongo.bson.BSONObjectID
import org.incal.core.runnables.InputFutureRunnableExt

import scala.reflect.runtime.universe.typeOf
import scala.concurrent.ExecutionContext.Implicits.global

class ShowDataSetIdsOfSpace @Inject()(
    dataSpaceMetaInfoRepo: DataSpaceMetaInfoRepo
  ) extends InputFutureRunnableExt[ShowDataSetIdsOfSpaceSpec] {

  private val logger = Logger

  override def runAsFuture(input: ShowDataSetIdsOfSpaceSpec) =
    for {
      dataSpace <- dataSpaceMetaInfoRepo.get(BSONObjectID.parse(input.dataSpaceId).get).map(_.get)
    } yield {
      val ids = dataSpace.dataSetMetaInfos.map(_.id)
      logger.info("Data set ids: " + ids.mkString(", "))
    }
}

case class ShowDataSetIdsOfSpaceSpec(
  dataSpaceId: String
)