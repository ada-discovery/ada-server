package org.ada.server.dataaccess.mongo

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import javax.inject.Inject
import play.api.libs.json._
import reactivemongo.api.indexes.{Index, IndexType}
import reactivemongo.akkastream.{AkkaStreamCursor, State}
import reactivemongo.api.{Cursor, _}

import scala.concurrent.Future
import org.incal.core.{Identity, dataaccess}
import org.incal.core.dataaccess._
import org.incal.core.akka.AkkaStreamUtil
import reactivemongo.play.json.collection.JSONBatchCommands
import JSONBatchCommands.AggregationFramework.GroupFunction
import akka.NotUsed

protected class MongoAsyncRepo[E: Format, ID: Format](
    collectionName : String)(
    implicit identity: Identity[E, ID]
  ) extends MongoAsyncReadonlyRepo[E, ID](collectionName, identity.name) with AsyncRepo[E, ID] {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  import reactivemongo.play.json._

  override def save(entity: E): Future[ID] = {
    val (doc, id) = toJsonAndId(entity)

    withCollection(
      _.insert(ordered = false).one(doc).map {
        case le if le.ok => id
        case le => throw new InCalDataAccessException(le.writeErrors.map(_.errmsg).mkString(". "))
      }.recover(handleExceptions)
    )
  }

  override def save(entities: Traversable[E]): Future[Traversable[ID]] = {
    val docAndIds = entities.map(toJsonAndId)

    withCollection(
      _.insert(ordered = false).many(docAndIds.map(_._1).toStream).map { // bulkSize = 100, bulkByteSize = 16793600
        case le if le.ok => docAndIds.map(_._2)
        case le => throw new InCalDataAccessException(le.errmsg.getOrElse(""))
      }.recover(handleExceptions)
    )
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
    withCollection(_.db.asInstanceOf[DefaultDB].runCommand(FSyncCommand(), FailoverStrategy.default).map(_ => ()))
}

class MongoAsyncCrudRepo[E: Format, ID: Format](
    collectionName : String)(
    implicit identity: Identity[E, ID]
  ) extends MongoAsyncRepo[E, ID](collectionName) with MongoAsyncCrudExtraRepo[E, ID] {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  import reactivemongo.play.json._
  import reactivemongo.play.json.collection.JsCursor._

  override def update(entity: E): Future[ID] = {
    val doc = Json.toJson(entity).as[JsObject]

    identity.of(entity).map { id =>
      withCollection(
        _.update(ordered = false).one(Json.obj(identity.name -> Json.toJson(id)), doc) map {
          case le if le.ok => id
          case le => throw new InCalDataAccessException(le.writeErrors.map(_.errmsg).mkString(". "))
        }
      )
    }.getOrElse(
      throw new InCalDataAccessException("Id required for update.")
    )
  }.recover(handleExceptions)

  // collection.remove(Json.obj(identity.name -> id), firstMatchOnly = true)
  override def delete(id: ID): Future[Unit] = withCollection {
    _.delete(ordered = false).one(Json.obj(identity.name -> Json.toJson(id))) map handleResult
  }.recover(handleExceptions)

  override def deleteAll: Future[Unit] = withCollection {
    _.delete(ordered = false).one(Json.obj()) map handleResult
  }.recover(handleExceptions)

  // extra functions which should not be exposed beyond the persistence layer
  override protected[dataaccess] def updateCustom(
    selector: JsObject,
    modifier: JsObject
  ): Future[Unit] = withCollection {
    _.update(ordered = false).one(selector, modifier) map handleResult
  }.recover(handleExceptions)

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
  ): Future[Traversable[JsObject]] = withCollection { collection =>
    import collection.BatchCommands.AggregationFramework._
    import collection.BatchCommands.AggregationFramework.{Sort => AggSort}

    val jsonRootCriteria = rootCriteria.headOption.map(_ => toMongoCriteria(rootCriteria))
    val jsonSubCriteria = subCriteria.headOption.map(_ => toMongoCriteria((subCriteria)))

    def toAggregateSort(sorts: Seq[dataaccess.Sort]) =
      sorts.map {
        _ match {
          case AscSort(fieldName) => Ascending(fieldName)
          case DescSort(fieldName) => Descending(fieldName)
        }
      }

    val params: List[PipelineOperator] = List(
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

    val cursor: reactivemongo.api.Cursor.WithOps[JsObject] = collection.aggregatorContext[JsObject](params.head, params.tail).prepared.cursor
    cursor.collect[List](-1, reactivemongo.api.Cursor.FailOnError[List[JsObject]]())
  }
}

trait MongoAsyncCrudExtraRepo[E, ID] extends AsyncCrudRepo[E, ID] {

  this: MongoAsyncReadonlyRepo[E, ID] =>

//  import collection.BatchCommands.AggregationFramework.GroupFunction

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
    modifier: JsObject
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

  private val maxSize = 1024000
  private val maxDocsSize = 10000

  override lazy val stream: Source[E, NotUsed] =
    AkkaStreamUtil.fromFutureSource(akkaCursor.map(_.documentSource()))

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

  private lazy val cappedCollection: Future[JSONCollection] = withCollection { collection =>
    collection.stats().flatMap {
      case stats if !stats.capped =>
        // The collection is not capped, so we convert it
        collection.convertToCapped(maxSize, Some(maxDocsSize))
      case _ => Future(collection)
    }.recover {
      // The collection mustn't exist, create it
      case _ =>
        collection.createCapped(maxSize, Some(maxDocsSize))
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