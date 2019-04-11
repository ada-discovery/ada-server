package org.ada.server.dataaccess

import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}
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

object JsonCrudRepoExtra {

  implicit class CrudInfixOps(val dataSetRepo: JsonCrudRepo) extends AnyVal {

    def saveAsStream(
      source: Source[JsObject, _],
      spec: StreamSpec = StreamSpec())(
      implicit materializer: Materializer
    ): Future[Unit] = {

      val finalStream = asyncStream(
        source,
        dataSetRepo.save(_: JsObject),
        Some(dataSetRepo.save(_ : Traversable[JsObject])),
        spec
      )

      finalStream.runWith(Sink.ignore).map(_ => ())
    }

    def updateAsStream(
      source: Source[JsObject, _],
      spec: StreamSpec = StreamSpec())(
      implicit materializer: Materializer
    ): Future[Unit] = {

      val finalStream = asyncStream(
        source,
        dataSetRepo.update(_: JsObject),
        Some(dataSetRepo.update(_ : Traversable[JsObject])),
        spec
      )

      finalStream.runWith(Sink.ignore).map(_ => ())
    }
  }

  private def asyncStream[T, U](
    source: Source[T, _],
    process: T => Future[U],
    batchProcess: Option[Traversable[T] => Future[Traversable[U]]] = None,
    spec: StreamSpec = StreamSpec())(
    implicit materializer: Materializer
  ): Source[U, _] = {
    val parallelismInit = spec.parallelism.getOrElse(1)

    def buffer[T](stream: Source[T, _]): Source[T, _] =
      spec.backpressureBufferSize.map(stream.buffer(_, OverflowStrategy.backpressure)).getOrElse(stream)

    val batchProcessInit = batchProcess.getOrElse((values: Traversable[T]) => Future.sequence(values.map(process)))

    spec.batchSize match {

      // batch size is defined
      case Some(batchSize) =>
        buffer(source.grouped(batchSize))
          .mapAsync(parallelismInit)(batchProcessInit).mapConcat(_.toList)

      case None =>
        buffer(source)
          .mapAsync(parallelismInit)(process)
    }
  }
}

case class StreamSpec(
  batchSize: Option[Int] = None,
  backpressureBufferSize: Option[Int]  = None,
  parallelism: Option[Int] = None
)