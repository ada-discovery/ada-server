package org.ada.server.dataaccess.dataset

import com.google.inject.ImplementedBy
import org.ada.server.dataaccess.RepoTypes._
import org.ada.server.dataaccess.ignite.DataViewCacheCrudRepoFactory
import org.ada.server.models.DataView
import org.ada.server.models.User.UserIdentity
import org.incal.core.dataaccess.Criterion.Infix

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@ImplementedBy(classOf[DataViewCacheCrudRepoFactory])
trait DataViewRepoFactory {
  def apply(dataSetId: String): DataViewRepo
}

object DataViewRepo {

  def setCreatedBy(
    userRepo: UserRepo,
    dataViews: Traversable[DataView]
  ): Future[Unit] = {
    val userIds = dataViews.map(_.createdById).flatten.map(Some(_)).toSeq

    if (userIds.nonEmpty) {
      userRepo.find(Seq(UserIdentity.name #-> userIds)).map { users =>
        val userIdMap = users.map(c => (c._id.get, c)).toMap
        dataViews.foreach(dataView =>
          if (dataView.createdById.isDefined) {
            dataView.createdBy = userIdMap.get(dataView.createdById.get)
          }
        )
      }
    } else
      Future(())
  }
}