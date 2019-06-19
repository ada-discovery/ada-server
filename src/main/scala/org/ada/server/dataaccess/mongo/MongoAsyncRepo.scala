package org.ada.server.dataaccess.mongo

import javax.inject.Inject
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import play.api.Logger
import play.api.libs.json.Json.JsValueWrapper
import play.api.libs.json._
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.commands.WriteResult
import reactivemongo.api.indexes.{Index, IndexType}
import reactivemongo.play.json.collection.JSONBatchCommands.JSONCountCommand.Count
import reactivemongo.akkastream.{AkkaStreamCursor, State}
import reactivemongo.api._
import reactivemongo.core.actors.Exceptions.PrimaryUnavailableException
import reactivemongo.core.commands.RawCommand
import reactivemongo.core.errors.ReactiveMongoException
import org.ada.server.dataaccess.ignite.BinaryJsonUtil.toJson

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import org.incal.core.Identity
import org.incal.core.dataaccess._

import scala.util.Random

protected class MongoAsyncReadonlyRepo[E: Format, ID: Format](
    collectionName : String,
    val identityName : String,
    mongoAutoCreateIndexForProjection: Boolean = false
  ) extends AsyncReadonlyRepo[E, ID] {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  import play.modules.reactivemongo.json._
  import play.modules.reactivemongo.json.collection.JSONCollection

  private val indexNameMaxSize = 40
  private val indexMaxFieldsNum = 31
  protected val logger = Logger
  private implicit val system = ActorSystem()
  protected val materializer = ActorMaterializer()

  @Inject var reactiveMongoApi : ReactiveMongoApi = _

  //  private val failoverStrategy =
  //    FailoverStrategy(
  //      initialDelay = 5 seconds,
  //      retries = 5,
  //      delayFactor =
  //        attemptNumber => 1 + attemptNumber * 0.5
  //    )

  protected lazy val collection: JSONCollection = reactiveMongoApi.db.collection[JSONCollection](collectionName)

  override def get(id: ID): Future[Option[E]] =
    collection.find(Json.obj(identityName -> id)).one[E]

  override def find(
    criteria: Seq[Criterion[Any]],
    sort: Seq[Sort],
    projection: Traversable[String],
    limit: Option[Int],
    skip: Option[Int]
  ): Future[Traversable[E]] = {
    findAsCursor(criteria, sort, projection, limit, skip).flatMap ( cursor =>
      // handle the limit
      limit match {
        case Some(limit) => cursor.collect[List](limit)
        case None => cursor.collect[List]()
      }
    ).recover(handleExceptions)
  }

  override def findAsStream(
    criteria: Seq[Criterion[Any]],
    sort: Seq[Sort],
    projection: Traversable[String],
    limit: Option[Int],
    skip: Option[Int]
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
  ): Future[AkkaStreamCursor[E]] = {
    val sortedProjection = projection.toSeq.sorted
    val jsonProjection = JsObject(
      sortedProjection.map(fieldName => (fieldName, JsNumber(1)))
        ++
        (
          if (sortedProjection.nonEmpty && !sortedProjection.contains(identityName))
            Seq((identityName, JsNumber(0)))
          else
            Seq()
          )
    )
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
    val jsonSorts = sorts.map{
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

      case c: NotEqualsCriterion[T] => {
//        val json = c.value match {
//            case Some(value) => toJson(value)
//            case None => JsNull
//          }
        Json.obj("$ne" -> toJson(c.value))
      }

      case c: NotEqualsNullCriterion =>
        Json.obj("$ne" -> JsNull)

      case c: InCriterion[V] => {
        val inValues = c.value.map(toJson(_): JsValueWrapper)
        Json.obj("$in" -> Json.arr(inValues: _*))
      }

      case c: NotInCriterion[V] => {
        val inValues = c.value.map(toJson(_): JsValueWrapper)
        Json.obj("$nin" -> Json.arr(inValues: _*))
      }

      case c: GreaterCriterion[T] =>
        Json.obj("$gt" -> toJson(c.value))

      case c: GreaterEqualCriterion[T] =>
        Json.obj("$gte" -> toJson(c.value))

      case c: LessCriterion[T] =>
        Json.obj("$lt" -> toJson(c.value))

      case c: LessEqualCriterion[T] =>
        Json.obj("$lte" -> toJson(c.value))
    }

  override def count(criteria: Seq[Criterion[Any]]): Future[Int] =
    criteria match {
      case Nil => collection.count().recover(handleExceptions)
      case _ => {
        val jsonCriteria = toMongoCriteria(criteria)
        collection.runCommand(Count(jsonCriteria)).map(_.value).recover(handleExceptions)
      }
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

protected class MongoAsyncRepo[E: Format, ID: Format](
    collectionName : String)(
    implicit identity: Identity[E, ID]
  ) extends MongoAsyncReadonlyRepo[E, ID](collectionName, identity.name) with AsyncRepo[E, ID] {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  import play.modules.reactivemongo.json._

  override def save(entity: E): Future[ID] = {
    val (doc, id) = toJsonAndId(entity)

    collection.insert(doc).map {
      case le if le.ok => id
      case le => throw new InCalDataAccessException(le.writeErrors.map(_.errmsg).mkString(". "))
    }.recover(handleExceptions)
  }

  override def save(entities: Traversable[E]): Future[Traversable[ID]] = {
    val docAndIds = entities.map(toJsonAndId)

    collection.bulkInsert(docAndIds.map(_._1).toStream, ordered = false).map { // bulkSize = 100, bulkByteSize = 16793600
      case le if le.ok => docAndIds.map(_._2)
      case le => throw new InCalDataAccessException(le.errmsg.getOrElse(""))
    }.recover(handleExceptions)
  }

  private def toJsonAndId(entity: E): (JsObject, ID) = {
    val givenId = identity.of(entity)
    if (givenId.isDefined) {
      val doc = Json.toJson(entity).as[JsObject]
      (doc, givenId.get)
    } else {
      val id = identity.next
      val doc = Json.toJson(identity.set(entity, id)).as[JsObject]
      (doc, id)
    }
  }

  override def flushOps = {
    val rawCommand = RawCommand(FSyncCommand().toBSON)
    reactiveMongoApi.db.command(rawCommand).map(_ => ())
    //    collection.runCommand(FSyncCommand()).map(_ => ())
  }
}

class MongoAsyncCrudRepo[E: Format, ID: Format](
    collectionName : String)(
    implicit identity: Identity[E, ID]
  ) extends MongoAsyncRepo[E, ID](collectionName) with MongoAsyncCrudExtraRepo[E, ID] {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  import play.modules.reactivemongo.json._
  import play.modules.reactivemongo.json.commands.JSONAggregationFramework.{Match, Group, GroupFunction, Unwind, Sort => AggSort, Cursor => AggCursor, Limit, Skip, Project, SumField, Push, SortOrder, Ascending, Descending}

  override def update(entity: E): Future[ID] = {
    val doc = Json.toJson(entity).as[JsObject]
    identity.of(entity).map{ id =>
      collection.update(Json.obj(identity.name -> id), doc) map {
        case le if le.ok => id
        case le => throw new InCalDataAccessException(le.writeErrors.map(_.errmsg).mkString(". "))
      }
    }.getOrElse(
      throw new InCalDataAccessException("Id required for update.")
    )
  }.recover(
    handleExceptions
  )

  // collection.remove(Json.obj(identity.name -> id), firstMatchOnly = true)
  override def delete(id: ID): Future[Unit] = {
    collection.remove(Json.obj(identity.name -> id)) map handleResult
  }.recover(
    handleExceptions
  )

  override def deleteAll: Future[Unit] = {
    collection.remove(Json.obj()) map handleResult
  }.recover(handleExceptions)

  // extra functions which should not be exposed beyond the persistence layer
  override protected[dataaccess] def updateCustom(
    selector: JsObject,
    modifier : JsObject
  ): Future[Unit] =
    collection.update(selector, modifier)
      .recover(handleExceptions) map handleResult

  override protected[dataaccess] def findAggregate(
    rootCriteria: Seq[Criterion[Any]],
    subCriteria: Seq[Criterion[Any]],
    sort: Seq[Sort],
    projection : Option[JsObject],
    idGroup : Option[JsValue],
    groups : Option[Seq[(String, GroupFunction)]],
    unwindFieldName : Option[String],
    limit: Option[Int],
    skip: Option[Int]
  ): Future[Traversable[JsObject]] = {
    val jsonRootCriteria = rootCriteria.headOption.map(_ => toMongoCriteria(rootCriteria))
    val jsonSubCriteria = subCriteria.headOption.map(_ => toMongoCriteria((subCriteria)))

    val params = List(
      projection.map(Project(_)),                                     // $project // TODO: should add field names used in criteria to the projection
      jsonRootCriteria.map(Match(_)),                                 // $match
      unwindFieldName.map(Unwind(_)),                                 // $unwind
      jsonSubCriteria.map(Match(_)),                                  // $match
      sort.headOption.map(_ => AggSort(toAggregateSort(sort): _ *)),  // $sort
      skip.map(Skip(_)),                                              // $skip
      limit.map(Limit(_)),                                            // $limit
      idGroup.map(id => Group(id)(groups.get: _*))                    // $group
    ).flatten

    val result = collection.aggregate(params.head, params.tail, false, false).recover(handleExceptions)
    result.map(_.documents)

    // TODO: once "org.reactivemongo" %% "play2-reactivemongo" % "0.12.0-play24" is release use the following aggregate call, which uses cursor and should be more optimal
//        val cursor = AggCursor(batchSize = 1)
//
//        val result = collection.aggregate1[JsObject](params.head, params.tail, cursor)
//        result.flatMap(_.collect[List]())
  }

  private def toAggregateSort(sorts: Seq[Sort]) =
    sorts.map{
      _ match {
        case AscSort(fieldName) => Ascending(fieldName)
        case DescSort(fieldName) => Descending(fieldName)
      }}
}

trait MongoAsyncCrudExtraRepo[E, ID] extends AsyncCrudRepo[E, ID] {
  import play.modules.reactivemongo.json.commands.JSONAggregationFramework.{GroupFunction, SortOrder}

  /*
   * Special aggregate function closely tight to Mongo db functionality.
   *
   * Should be used only for special cases (only within the persistence layer)!
   */
  protected[dataaccess] def findAggregate(
    rootCriteria: Seq[Criterion[Any]],
    subCriteria: Seq[Criterion[Any]],
    sort: Seq[Sort],
    projection : Option[JsObject],
    idGroup : Option[JsValue],
    groups : Option[Seq[(String, GroupFunction)]],
    unwindFieldName : Option[String],
    limit: Option[Int],
    skip: Option[Int]
  ): Future[Traversable[JsObject]]

  /*
   * Special update function expecting a modifier specified as a JSON object closely tight to Mongo db functionality
   *
   * should be used only for special cases (only within the persistence layer)!
   */
  protected[dataaccess] def updateCustom(
    selector: JsObject,
    modifier : JsObject
  ): Future[Unit]
}

class MongoAsyncStreamRepo[E: Format, ID: Format](
    collectionName : String,
    timestampFieldName: Option[String] = None)(
    implicit identity: Identity[E, ID]
  ) extends MongoAsyncRepo[E, ID](collectionName) with AsyncStreamRepo[E, ID] {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  import play.modules.reactivemongo.json._
  import play.modules.reactivemongo.json.collection.JSONCollection

  private implicit val m = materializer

  override lazy val stream: Source[E, Future[State]] = {
    val futureSource = akkaCursor.map(_.documentSource())
    Await.result(futureSource, 1 minute)
  }

//  override lazy val oldStream: Enumerator[E] = {
//    val enumerator = Enumerator.flatten(akkaCursor.map(_.enumerate()))
//    Concurrent.broadcast(enumerator)._1
//  }

  import reactivemongo.akkastream.cursorProducer

  private def akkaCursor: Future[AkkaStreamCursor[E]] = {
//    val since = BSONObjectID.generate
//    val criteria = Json.obj("_id" -> Json.obj("$gt" -> since))
//    Json.obj("$natural" -> 1)
    val criteria = timestampFieldName.map(fieldName =>
      Json.obj(fieldName -> Json.obj("$gt" -> new java.util.Date().getTime))
    ).getOrElse(
      Json.obj()
    )

    for {
      coll <- cappedCollection
    } yield
      coll.find(criteria).options(QueryOpts().tailable.awaitData).cursor[E]()
  }

  private lazy val cappedCollection: Future[JSONCollection] = {
    collection.stats().flatMap {
      case stats if !stats.capped =>
        // The collection is not capped, so we convert it
        collection.convertToCapped(102400, Some(1000))
      case _ => Future(collection)
    }.recover {
      // The collection mustn't exist, create it
      case _ =>
        collection.createCapped(102400, Some(1000))
    }.flatMap( _ =>
      if (timestampFieldName.isDefined) {
        collection.indexesManager.ensure(Index(
          key = Seq(timestampFieldName.get -> IndexType.Ascending),
          unique = true
        )).map(_ => collection)
      } else
        Future(collection)
    )
  }
}

object CriteriaJSONWriter extends Writes[Map[String, Any]] {
  override def writes(criteria: Map[String, Any]): JsObject = JsObject(criteria.mapValues(toJsValue(_)).toSeq)
  val toJsValue: PartialFunction[Any, JsValue] = {
    case v: String => JsString(v)
    case v: Int => JsNumber(v)
    case v: Long => JsNumber(v)
    case v: Double => JsNumber(v)
    case v: Boolean => JsBoolean(v)
    case obj: JsValue => obj
    case map: Map[String, Any] @unchecked => CriteriaJSONWriter.writes(map)
    case coll: Traversable[_] => JsArray(coll.map(toJsValue(_)).toSeq)
    case null => JsNull
    case other => throw new IllegalArgumentException(s"Criteria value type not supported: $other")
  }
}