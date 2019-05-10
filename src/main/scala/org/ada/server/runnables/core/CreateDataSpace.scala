package org.ada.server.runnables.core

import javax.inject.Inject

import org.ada.server.dataaccess.RepoTypes.DataSpaceMetaInfoRepo
import org.ada.server.models.DataSpaceMetaInfo
import reactivemongo.bson.BSONObjectID
import org.incal.core.runnables.InputFutureRunnable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.runtime.universe.typeOf

class CreateDataSpace @Inject() (repo: DataSpaceMetaInfoRepo) extends InputFutureRunnable[DataSpaceSpec] {

  override def runAsFuture(input: DataSpaceSpec) = {
    val sortOrder = input.sortOrder.getOrElse(0)

    // create a new data space
    val dataSpace = DataSpaceMetaInfo(_id = None, name = input.name, sortOrder = sortOrder, parentId = input.parentId)

    // save
    repo.save(dataSpace).map(_ => ())
  }

  override def inputType = typeOf[DataSpaceSpec]
}

case class DataSpaceSpec(name: String, sortOrder: Option[Int], parentId: Option[BSONObjectID])