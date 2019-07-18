package org.ada.server.dataaccess.mongo

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import javax.inject.Inject
import play.api.libs.json._
import reactivemongo.api.indexes.{Index, IndexType}
import reactivemongo.akkastream.{AkkaStreamCursor, State}
import reactivemongo.api._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import org.incal.core.{Identity, dataaccess}
import org.incal.core.dataaccess._

protected class MongoAsyncRepo[E: Format, ID: Format](
    collectionName : String)(
    implicit identity: Identity[E, ID]
  ) extends MongoAsyncReadonlyRepo[E, ID](collectionName, identity.name) with AsyncRepo[E, ID] {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  import reactivemongo.play.json._

  override def save(entity: E): Future[ID] = {
    val (doc, id) = toJsonAndId(entity)

    collection.insert(ordered = false).one(doc).map {
      case le if le.ok => id
      case le => throw new InCalDataAccessException(le.writeErrors.map(_.errmsg).mkString(". "))
    }.recover(handleExceptions)
  }

  override def save(entities: Traversable[E]): Future[Traversable[ID]] = {
    val docAndIds = entities.map(toJsonAndId)

    collection.insert(ordered = false).many(docAndIds.map(_._1).toStream).map { // bulkSize = 100, bulkByteSize = 16793600
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

  import FSyncCommand._

  override def flushOps =
    collection.db.asInstanceOf[DefaultDB].runCommand(FSyncCommand(), FailoverStrategy.default).map(_ => ())
}

class MongoAsyncCrudRepo[E: Format, ID: Format](
    collectionName : String)(
    implicit identity: Identity[E, ID]
  ) extends MongoAsyncRepo[E, ID](collectionName) with MongoAsyncCrudExtraRepo[E, ID] {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  import reactivemongo.play.json._
  import collection.BatchCommands.AggregationFramework._
  import collection.BatchCommands.AggregationFramework.{Sort => AggSort}
  import reactivemongo.play.json.collection.JsCursor._

  //  import reactivemongo.play.json.commands.JSONAggregationFramework.{Match, Group, GroupFunction, Unwind, Sort => AggSort, Cursor => AggCursor, Limit, Skip, Project, SumField, Push, SortOrder, Ascending, Descending}

  override def update(entity: E): Future[ID] = {
    val doc = Json.toJson(entity).as[JsObject]

    identity.of(entity).map{ id =>
      collection.update(ordered = false).one(Json.obj(identity.name -> Json.toJson(id)), doc) map {
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
    collection.delete(ordered = false).one(Json.obj(identity.name -> Json.toJson(id))) map handleResult
  }.recover(
    handleExceptions
  )

  override def deleteAll: Future[Unit] = {
    collection.delete(ordered = false).one(Json.obj()) map handleResult
  }.recover(handleExceptions)

  // extra functions which should not be exposed beyond the persistence layer
  override protected[dataaccess] def updateCustom(
    selector: JsObject,
    modifier: JsObject
  ): Future[Unit] =
    collection.update(ordered = false).one(selector, modifier)
      .recover(handleExceptions) map handleResult

  override protected[dataaccess] def findAggregate(
    rootCriteria: Seq[Criterion[Any]],
    subCriteria: Seq[Criterion[Any]],
    sort: Seq[dataaccess.Sort],
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

    import reactivemongo.bson._

    val cursor = collection.aggregatorContext[JsObject](params.head, params.tail).prepared.cursor
    cursor.collect[List](-1, reactivemongo.api.Cursor.FailOnError[List[JsObject]]())
  }

  private def toAggregateSort(sorts: Seq[dataaccess.Sort]) =
    sorts.map {
      _ match {
        case AscSort(fieldName) => Ascending(fieldName)
        case DescSort(fieldName) => Descending(fieldName)
      }
    }
}

trait MongoAsyncCrudExtraRepo[E, ID] extends AsyncCrudRepo[E, ID] {

  this: MongoAsyncReadonlyRepo[E, ID] =>

  import collection.BatchCommands.AggregationFramework.GroupFunction

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
  import reactivemongo.play.json._
  import reactivemongo.play.json.collection.JSONCollection

  @Inject implicit var materializer: Materializer = _

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
          unique = false
        )).map(_ => collection)
      } else
        Future(collection)
    )
  }
}