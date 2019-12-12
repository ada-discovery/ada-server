package org.ada.server.dataaccess

import org.ada.server.dataaccess.RepoTypes.{JsonCrudRepo, JsonReadonlyRepo}
import org.ada.server.models.DataSetFormattersAndIds.JsObjectIdentity
import play.api.libs.json.{JsLookupResult, JsObject}
import reactivemongo.play.json.BSONFormats._
import reactivemongo.bson.BSONObjectID
import org.incal.core.dataaccess._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object JsonReadonlyRepoExtra {

  private val idName = JsObjectIdentity.name

  implicit class ReadonlyInfixOps(val dataSetRepo: JsonReadonlyRepo) extends AnyVal {

    import Criterion.Infix

    def allIds: Future[Traversable[BSONObjectID]] =
      dataSetRepo.find(
        projection = Seq(idName)
      ).map { jsons =>
        val ids  = jsons.map(json => (json \ idName).as[BSONObjectID])
        ids.toSeq.sortBy(_.stringify)
      }

    @Deprecated
    def findByIds(
      firstId: BSONObjectID,
      batchSize: Int,
      projection: Traversable[String]
    ): Future[Traversable[JsObject]] =
      dataSetRepo.find(
        criteria = Seq(idName #>= firstId),
        limit = Some(batchSize),
        sort = Seq(AscSort(idName)),
        projection = projection
      )

    def max(
      fieldName: String,
      criteria: Seq[Criterion[Any]] = Nil,
      addNotNullCriterion: Boolean = false
    ): Future[Option[JsLookupResult]] =
      dataSetRepo.find(
        criteria = criteria ++ (if(addNotNullCriterion) Seq(NotEqualsNullCriterion(fieldName)) else Nil),
        projection = Seq(fieldName),
        sort = Seq(DescSort(fieldName)),
        limit = Some(1)
      ).map(_.headOption.map(_ \ fieldName))

    def min(
      fieldName: String,
      criteria: Seq[Criterion[Any]] = Nil,
      addNotNullCriterion: Boolean = false
    ): Future[Option[JsLookupResult]] =
      dataSetRepo.find(
        criteria = criteria ++ (if(addNotNullCriterion) Seq(NotEqualsNullCriterion(fieldName)) else Nil),
        projection = Seq(fieldName),
        sort = Seq(AscSort(fieldName)),
        limit = Some(1)
      ).map(_.headOption.map(_ \ fieldName))
  }
}