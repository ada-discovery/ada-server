package org.ada.server.dataaccess.dataset

import com.google.inject.ImplementedBy
import org.ada.server.dataaccess.RepoTypes._
import org.ada.server.dataaccess.ignite.FieldCacheCrudRepoFactory
import org.ada.server.models.DataSetFormattersAndIds.CategoryIdentity
import org.ada.server.models.Field
import org.incal.core.dataaccess.Criterion.Infix

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@ImplementedBy(classOf[FieldCacheCrudRepoFactory])
trait FieldRepoFactory {
  def apply(dataSetId: String): FieldRepo
}

object FieldRepo {

  def setCategoriesById(
    categoryRepo: CategoryRepo,
    fields: Traversable[Field]
  ): Future[Unit] = {
    val categoryIds = fields.map(_.categoryId).flatten.map(Some(_)).toSeq

    if (categoryIds.nonEmpty) {
      categoryRepo.find(Seq(CategoryIdentity.name #-> categoryIds)).map { categories =>
        val categoryIdMap = categories.map(c => (c._id.get, c)).toMap
        fields.foreach(field =>
          if (field.categoryId.isDefined) {
            field.category = categoryIdMap.get(field.categoryId.get)
          }
        )
      }
    } else
      Future(())
  }

  def setCategoryById(
    categoryRepo: CategoryRepo,
    field: Field
  ): Future[Unit] =
    field.categoryId.fold(Future(())) {
      categoryId =>
        categoryRepo.get(categoryId).map { category =>
          field.category = category
        }
    }

  // search for a category with a given name (if multiple, select the first one)
  def setCategoryByName(
    categoryRepo: CategoryRepo,
    field: Field
  ): Future[Unit] =
    field.category match {
      case Some(category) =>
        categoryRepo.find(Seq("name" #== category.name)).map(categories =>
          if (categories.nonEmpty) {
            val loadedCategory = categories.head
            field.category = Some(loadedCategory)
            field.categoryId = loadedCategory._id
          }
        )
      case None => Future(())
    }
}