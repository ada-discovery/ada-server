package org.ada.server.dataaccess

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import org.ada.server.dataaccess.RepoTypes.{JsonCrudRepo, JsonReadonlyRepo}
import play.api.libs.json.{Format, JsObject, Json}
import reactivemongo.bson.BSONObjectID
import org.incal.core.dataaccess._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private[dataaccess] abstract class AbstractJsonFormatReadonlyRepoAdapter[E: Format, ID] extends JsonReadonlyRepo {

  // hooks
  type REPO <: AsyncReadonlyRepo[E, ID]
  def repo: REPO

  override def find(
    criteria: Seq[Criterion[Any]],
    sort: Seq[Sort],
    projection: Traversable[String],
    limit: Option[Int],
    skip: Option[Int]
  ) =
    for {
      items <- repo.find(criteria, sort, projection, limit, skip)
    } yield
      items.map(asJson)

  override def findAsStream(
    criteria: Seq[Criterion[Any]],
    sort: Seq[Sort],
    projection: Traversable[String],
    limit: Option[Int],
    skip: Option[Int])(
    implicit materializer: Materializer
  ): Future[Source[JsObject, _]] =
    for {
      source <- repo.findAsStream(criteria, sort, projection, limit, skip)
    } yield
      source.map(asJson)

  override def count(criteria: Seq[Criterion[Any]]) =
    repo.count(criteria)

  protected def asJson(item: E) =
    Json.toJson(item).as[JsObject]
}

private[dataaccess] class JsonFormatReadonlyRepoAdapter[E: Format](
    val repo: AsyncReadonlyRepo[E, BSONObjectID]
  ) extends AbstractJsonFormatReadonlyRepoAdapter[E, BSONObjectID] {

  override type REPO = AsyncReadonlyRepo[E, BSONObjectID]

  override def get(id: BSONObjectID) =
    for {
      item <- repo.get(id)
    } yield
      item.map(asJson)
}

private[dataaccess] class NoIdJsonFormatReadonlyRepoAdapter[E: Format, ID](
    val repo: AsyncReadonlyRepo[E, ID]
  ) extends AbstractJsonFormatReadonlyRepoAdapter[E, ID] {

  override type REPO = AsyncReadonlyRepo[E, ID]

  override def get(id: BSONObjectID) =
    throw new InCalDataAccessException("This instance of JSON format readonly adapter does not support id-based operations such as \"get(id)\"")
}

private[dataaccess] abstract class AbstractJsonFormatCrudRepoAdapter[E: Format, ID]
  extends AbstractJsonFormatReadonlyRepoAdapter[E, ID] with JsonCrudRepo {

  override type REPO <: AsyncCrudRepo[E, ID]

  override def update(entity: JsObject) =
    repo.update(entity.as[E]).map(toOutputId)

  override def deleteAll =
    repo.deleteAll

  override def save(entity: JsObject) =
    repo.save(entity.as[E]).map(toOutputId)

  protected def toOutputId(id: ID): BSONObjectID
}

private[dataaccess] class JsonFormatCrudRepoAdapter[E: Format](
    val repo: AsyncCrudRepo[E, BSONObjectID]
  ) extends AbstractJsonFormatCrudRepoAdapter[E, BSONObjectID] {

  override type REPO = AsyncCrudRepo[E, BSONObjectID]

  override def get(id: BSONObjectID) =
    for {
      item <- repo.get(id)
    } yield
      item.map(asJson)

  override def delete(id: BSONObjectID) =
    repo.delete(id)

  override protected def toOutputId(id: BSONObjectID) = id

  override def flushOps = repo.flushOps
}

private[dataaccess] class NoIdJsonFormatCrudRepoAdapter[E: Format, ID](
    val repo: AsyncCrudRepo[E, ID]
  ) extends AbstractJsonFormatCrudRepoAdapter[E, ID] {

  override type REPO = AsyncCrudRepo[E, ID]

  override def get(id: BSONObjectID) =
    throw new InCalDataAccessException("This instance of JSON format crud adapter does not support id-based operations such as \"get(id)\"")

  override def delete(id: BSONObjectID) =
    throw new InCalDataAccessException("This instance of JSON format crud adapter does not support id-based operations such as \"delete(id)\"")

  // TODO: this should be returned with a warning since the BSON object id is generated randomly
  override protected def toOutputId(id: ID): BSONObjectID =
    BSONObjectID.generate

  override def flushOps = repo.flushOps
}

object JsonFormatRepoAdapter {
  def apply[T: Format](repo: AsyncReadonlyRepo[T, BSONObjectID]): JsonReadonlyRepo =
    new JsonFormatReadonlyRepoAdapter(repo)

  def apply[T: Format](repo: AsyncCrudRepo[T, BSONObjectID]): JsonCrudRepo =
    new JsonFormatCrudRepoAdapter(repo)

  def applyNoId[T: Format](repo: AsyncReadonlyRepo[T, _]): JsonReadonlyRepo =
    new NoIdJsonFormatReadonlyRepoAdapter(repo)

  def applyNoId[T: Format](repo: AsyncCrudRepo[T, _]): JsonCrudRepo =
    new NoIdJsonFormatCrudRepoAdapter(repo)
}