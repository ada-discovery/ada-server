package org.ada.server.dataaccess

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import org.incal.core.Identity
import org.incal.core.dataaccess.{AsyncCrudRepo, Criterion, Sort}
import org.incal.core.dataaccess.Criterion._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private class SubTypeBasedAsyncCrudRepoAdapter[SUB_E: Manifest, E >: SUB_E, ID](
  underlying: AsyncCrudRepo[E, ID]
) extends AsyncCrudRepo[SUB_E, ID] {

  private val concreteClassFieldName = "concreteClass"
  private val targetClassName = manifest[SUB_E].runtimeClass.getName

  private val targetClassCriterion = concreteClassFieldName #== targetClassName

  // READONLY / SEARCH OPERATIONS

  override def get(id: ID): Future[Option[SUB_E]] =
    for {
      itemOption <- underlying.get(id)
    } yield
      itemOption.flatMap(item =>
        if (item.isInstanceOf[SUB_E]) Some(item.asInstanceOf[SUB_E]) else None
      )

  override def exists(id: ID): Future[Boolean] =
    underlying.exists(id)

  override def find(
    criteria: Seq[Criterion[Any]],
    sort: Seq[Sort],
    projection: Traversable[String],
    limit: Option[Int],
    skip: Option[Int]
  ): Future[Traversable[SUB_E]] =
    for {
      items <- underlying.find(criteria ++ Seq(targetClassCriterion), sort, projection, limit, skip)
    } yield {
      items.map(_.asInstanceOf[SUB_E])
    }

  override def findAsStream(
    criteria: Seq[Criterion[Any]],
    sort: Seq[Sort],
    projection: Traversable[String],
    limit: Option[Int],
    skip: Option[Int])(
    implicit materializer: Materializer
  ): Future[Source[SUB_E, _]] =
    for {
      items <- underlying.findAsStream(criteria ++ Seq(targetClassCriterion), sort, projection, limit, skip)
    } yield {
      items.map(_.asInstanceOf[SUB_E])
    }

  override def count(criteria: Seq[Criterion[Any]]): Future[Int] =
    underlying.count(criteria ++ Seq(targetClassCriterion))


  // SAVE OPERATIONS

  override def save(entity: SUB_E): Future[ID] =
    underlying.save(entity)

  override def save(entities: Traversable[SUB_E]): Future[Traversable[ID]] =
    underlying.save(entities)

  override def update(entity: SUB_E): Future[ID] =
    underlying.update(entity)

  override def update(entities: Traversable[SUB_E]): Future[Traversable[ID]] =
    underlying.update(entities)

  override def delete(id: ID): Future[Unit] =
    underlying.delete(id)

  override def delete(ids: Traversable[ID]): Future[Unit] =
    underlying.delete(ids)

  // TODO: need a list of ids... add a projection method to asyncCrudRepo
  override def deleteAll: Future[Unit] = ???
//    for {
//      ids <- find(criteria = Seq(targetClassCriterion), projection = Seq(identity.name))
//      _ <- delete(ids)
//    }

  override def flushOps: Future[Unit] = underlying.flushOps
}

object SubTypeBasedAsyncCrudRepo {

  def apply[E_SUB: Manifest, E >: E_SUB, ID](
    underlying: AsyncCrudRepo[E, ID]
  ): AsyncCrudRepo[E_SUB, ID] =
    new SubTypeBasedAsyncCrudRepoAdapter[E_SUB, E, ID](underlying)
}
