package org.ada.server.dataaccess.dataset

import com.google.inject.ImplementedBy
import org.ada.server.dataaccess.RepoTypes.CategoryRepo
import org.ada.server.dataaccess.ignite.CategoryCacheCrudRepoFactory
import org.ada.server.models.Category
import reactivemongo.bson.BSONObjectID

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@ImplementedBy(classOf[CategoryCacheCrudRepoFactory])
trait CategoryRepoFactory {
  def apply(dataSetId: String): CategoryRepo
}

object CategoryRepo {

  def saveRecursively(
    categoryRepo: CategoryRepo,
    category: Category
  ): Future[Seq[(Category, BSONObjectID)]] = {
    val children = category.children
    category.children = Nil
    categoryRepo.save(category).flatMap { id =>
      val idsFuture = children.map { child =>
        child.parentId = Some(id)
        saveRecursively(categoryRepo, child)
      }
      Future.sequence(idsFuture).map(ids => Seq((category, id)) ++ ids.flatten)
    }
  }
}