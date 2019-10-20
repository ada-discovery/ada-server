package org.ada.server.dataaccess.mongo

import javax.inject.Inject
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Source
import play.api.Logger
import play.api.libs.json.Json.JsValueWrapper
import play.api.libs.json._
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.commands.WriteResult
import reactivemongo.api.indexes.{Index, IndexType}
import reactivemongo.akkastream.{AkkaStreamCursor, State}
import reactivemongo.play.json.JsObjectDocumentWriter
import reactivemongo.play.json.collection._
import reactivemongo.play.json.collection.JSONCollection
import reactivemongo.core.actors.Exceptions.PrimaryUnavailableException
import reactivemongo.core.errors.ReactiveMongoException
import org.ada.server.dataaccess.ignite.BinaryJsonUtil.toJson
import play.api.libs.concurrent.Execution.Implicits.defaultContext

import scala.concurrent.Future
import org.incal.core.dataaccess._
import reactivemongo.api.{Cursor, QueryOpts}

import scala.util.Random

protected class MongoAsyncReadonlyRepo[E: Format, ID: Format](
  collectionName : String,
  val identityName : String,
  mongoAutoCreateIndexForProjection: Boolean = false
) extends AsyncReadonlyRepo[E, ID] {

  private val indexNameMaxSize = 40
  private val indexMaxFieldsNum = 31
  protected val logger = Logger

  @Inject var reactiveMongoApi: ReactiveMongoApi = _

  //  private val failoverStrategy =
  //    FailoverStrategy(
  //      initialDelay = 5 seconds,
  //      retries = 5,
  //      delayFactor =
  //        attemptNumber => 1 + attemptNumber * 0.5
  //    )

  protected def collection: Future[JSONCollection] =
    reactiveMongoApi.database.map(
      _.collection[JSONCollection](collectionName)
    )

  protected def withCollection[T](fun: JSONCollection => Future[T]) = collection.flatMap(fun)

  override def get(id: ID): Future[Option[E]] =
    withCollection(
      _.find(Json.obj(identityName -> id), None).one[E]
    )

  override def find(
    criteria: Seq[Criterion[Any]],
    sort: Seq[Sort],
    projection: Traversable[String],
    limit: Option[Int],
    skip: Option[Int]
  ): Future[Traversable[E]] =
    findAsCursor(criteria, sort, projection, limit, skip).flatMap { cursor =>
      // handle the limit
      cursor.collect[List](limit.getOrElse(-1), Cursor.FailOnError[List[E]]())
    }.recover(handleExceptions)

  override def findAsStream(
    criteria: Seq[Criterion[Any]],
    sort: Seq[Sort],
    projection: Traversable[String],
    limit: Option[Int],
    skip: Option[Int])(
    implicit materializer: Materializer
  ): Future[Source[E, _]] =
    findAsCursor(criteria, sort, projection, limit, skip).map { cursor =>
      // handle the limit
      limit match {
        case Some(limit) => cursor.documentSource(limit)(materializer)
        case None => cursor.documentSource()(materializer)
      }
    }.recover(handleExceptions)

  import reactivemongo.akkastream.cursorProducer

  protected def findAsCursor(
    criteria: Seq[Criterion[Any]],
    sort: Seq[Sort],
    projection: Traversable[String],
    limit: Option[Int],
    skip: Option[Int]
  ): Future[AkkaStreamCursor[E]] = withCollection { collection =>
    val sortedProjection = projection.toSeq.sorted

    val jsonProjection = sortedProjection match {
      case Nil =>  None
      case _ => Some(JsObject(
        sortedProjection.map(fieldName => (fieldName, JsNumber(1)))
          ++
          (if (!sortedProjection.contains(identityName)) Seq((identityName, JsNumber(0))) else Nil)
      ))
    }

    val jsonCriteria = toMongoCriteria(criteria)

    // handle criteria and projection (if any)
    val queryBuilder = collection.find(jsonCriteria, jsonProjection)

    // use index / hint only if the limit is not provided and projection is not empty
    val queryBuilder2 =
      if (mongoAutoCreateIndexForProjection && limit.isEmpty && projection.nonEmpty && projection.size <= indexMaxFieldsNum) {
        val fullName = sortedProjection.mkString("_")
        val name = if (fullName.size <= indexNameMaxSize)
          fullName
        else {
          val num = Random.nextInt()
          fullName.substring(0, indexNameMaxSize - 10) + Math.max(num, -num)
        }
        val index = Index(
          sortedProjection.map((_, IndexType.Ascending)),
          Some(name)
        )

        for {
          _ <- collection.indexesManager.ensure(index)
        } yield {
          val jsonHint = JsObject(sortedProjection.map(fieldName => (fieldName, JsNumber(1))))
          queryBuilder.hint(jsonHint)
        }
      } else
        Future(queryBuilder)

    // handle sort (if any)
    val finalQueryBuilderFuture = sort match {
      case Nil => queryBuilder2
      case _ => queryBuilder2.map(_.sort(toJsonSort(sort)))
    }

    finalQueryBuilderFuture.map { finalQueryBuilder =>
      // handle pagination (if requested)
      limit match {
        case Some(limit) =>
          finalQueryBuilder.options(QueryOpts(skip.getOrElse(0), limit)).cursor[E]()

        case None =>
          if (skip.isDefined)
            throw new IllegalArgumentException("Limit is expected when skip is provided.")
          else
            finalQueryBuilder.cursor[E]()
      }
    }
  }

  private def toJsonSort(sorts: Seq[Sort]) = {
    val jsonSorts = sorts.map {
      _ match {
        case AscSort(fieldName) => (fieldName -> JsNumber(1))
        case DescSort(fieldName) => (fieldName -> JsNumber(-1))
      }}
    JsObject(jsonSorts)
  }

  protected def toMongoCriteria(criteria: Seq[Criterion[Any]]): JsObject = {
    val individualCriteria = criteria.map( criterion =>
      (criterion.fieldName, toMongoCondition(criterion))
    )
    if (individualCriteria.length < 2) {
      JsObject(individualCriteria)
    } else {
      val elements = individualCriteria.map { case (fieldName, json) => Json.obj(fieldName -> json) }
      Json.obj("$and" -> JsArray(elements))
    }
  }

  protected def toMongoCondition[T, V](criterion: Criterion[T]): JsObject =
    criterion match {
      case c: EqualsCriterion[T] =>
        Json.obj("$eq" -> toJson(c.value))
      //     {
      //       c.value match {
      //         case Some(value) => toJson(value)
      //         case None => JsNull
      //       }
      //     }
      case c: EqualsNullCriterion =>
        Json.obj("$eq" -> JsNull)

      case RegexEqualsCriterion(_, value) =>
        Json.obj("$regex" -> value, "$options" -> "i")

      case RegexNotEqualsCriterion(_, value) =>
        Json.obj("$regex" -> s"^(?!${value})", "$options" -> "i")

      case c: NotEqualsCriterion[T] =>
        Json.obj("$ne" -> toJson(c.value))

      case c: NotEqualsNullCriterion =>
        Json.obj("$ne" -> JsNull)

      case c: InCriterion[V] =>
        val inValues = c.value.map(toJson(_): JsValueWrapper)
        Json.obj("$in" -> Json.arr(inValues: _*))

      case c: NotInCriterion[V] =>
        val inValues = c.value.map(toJson(_): JsValueWrapper)
        Json.obj("$nin" -> Json.arr(inValues: _*))

      case c: GreaterCriterion[T] =>
        Json.obj("$gt" -> toJson(c.value))

      case c: GreaterEqualCriterion[T] =>
        Json.obj("$gte" -> toJson(c.value))

      case c: LessCriterion[T] =>
        Json.obj("$lt" -> toJson(c.value))

      case c: LessEqualCriterion[T] =>
        Json.obj("$lte" -> toJson(c.value))
    }

  override def count(criteria: Seq[Criterion[Any]]): Future[Int] = {
    val jsonCriteria = criteria match {
      case Nil => None
      case _ => Some(toMongoCriteria(criteria))
    }

    // collection.runCommand(Count(jsonCriteria)).map(_.value).recover(handleExceptions)
    withCollection(_.count(jsonCriteria).recover(handleExceptions))
  }

  override def exists(id: ID): Future[Boolean] =
    count(Seq(EqualsCriterion(identityName, id))).map(_ > 0)

  protected def handleResult(result : WriteResult) =
    if (!result.ok) throw new InCalDataAccessException(result.writeErrors.map(_.errmsg).mkString(". "))

  protected def handleExceptions[A]: PartialFunction[Throwable, A] = {
    case e: PrimaryUnavailableException =>
      val message = "Mongo node is not available."
      logger.error(message, e)
      throw new InCalDataAccessException(message, e)

    case e: ReactiveMongoException =>
      val message = "Problem with Mongo DB detected."
      logger.error(message, e)
      throw new InCalDataAccessException(message, e)
  }
}