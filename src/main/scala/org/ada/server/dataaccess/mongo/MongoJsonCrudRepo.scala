package org.ada.server.dataaccess.mongo

import javax.cache.configuration.Factory
import com.google.inject.assistedinject.Assisted
import org.ada.server.models.FieldTypeSpec
import play.api.libs.json.{JsObject, Json}
import play.api.Configuration
import play.api.inject.ApplicationLifecycle
import play.modules.reactivemongo.{DefaultReactiveMongoApi, ReactiveMongoApi}
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json._
import org.incal.core.dataaccess.{AsyncCrudRepo, InCalDataAccessException}
import org.ada.server.dataaccess.RepoTypes.JsonCrudRepo
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat

import scala.concurrent.Future
import javax.inject.Inject
import org.ada.server.AdaException
import org.ada.server.models.DataSetFormattersAndIds.JsObjectIdentity
import reactivemongo.api.{DefaultDB, FailoverStrategy, MongoConnection}

class MongoJsonCrudRepo @Inject()(
    @Assisted collectionName : String,
    @Assisted fieldNamesAndTypes: Seq[(String, FieldTypeSpec)],
    @Assisted mongoAutoCreateIndexForProjection: Boolean
  ) extends MongoAsyncReadonlyRepo[JsObject, BSONObjectID](collectionName, JsObjectIdentity.name, mongoAutoCreateIndexForProjection) with JsonCrudRepo {

  override def save(entity: JsObject): Future[BSONObjectID] =
    withCollection { collection =>
      val (_, id) = addId(entity)

      collection.insert(ordered = false).one(entity).map {
        case le if le.ok => id
        case le => throw new InCalDataAccessException(le.writeErrors.map(_.errmsg).mkString(". "))
      }
    }

  override def save(entities: Traversable[JsObject]): Future[Traversable[BSONObjectID]] =
    withCollection { collection =>
      val docAndIds = entities.map(addId)

      collection.insert(ordered = false).many(docAndIds.map(_._1).toStream).map {
        case le if le.ok => docAndIds.map(_._2)
        case le => throw new InCalDataAccessException(le.errmsg.getOrElse(""))
      }
    }

  private def addId(entity: JsObject): (JsObject, BSONObjectID) = {
    val id = BSONObjectID.generate
    (entity ++ Json.obj(identityName -> id), id)
  }

  override def update(entity: JsObject): Future[BSONObjectID] =
    withCollection { collection =>
      val id = (entity \ identityName).as[BSONObjectID]
      collection.update(ordered = false).one(Json.obj(identityName -> id), entity) map {
        case le if le.ok => id
        case le => throw new InCalDataAccessException(le.writeErrors.map(_.errmsg).mkString(". "))
      }
    }

  override def delete(id: BSONObjectID) = withCollection(
    _.delete(ordered = false).one(Json.obj(identityName -> id)) map handleResult
  )

  override def deleteAll : Future[Unit] = withCollection(
    _.delete(ordered = false).one(Json.obj()) map handleResult
  )

  import FSyncCommand._

  override def flushOps = withCollection(
    _.db.asInstanceOf[DefaultDB].runCommand(FSyncCommand(), FailoverStrategy.default).map(_ => ())
  )
}

class MongoJsonRepoFactory(
    collectionName: String,
    createIndexForProjectionAutomatically: Boolean,
    configuration: Configuration,
    applicationLifecycle: ApplicationLifecycle
  ) extends Factory[AsyncCrudRepo[JsObject, BSONObjectID]] {

  override def create(): AsyncCrudRepo[JsObject, BSONObjectID] = {
    val repo = new MongoJsonCrudRepo(collectionName, Nil, createIndexForProjectionAutomatically)
    repo.reactiveMongoApi = ReactiveMongoApi.create(configuration, applicationLifecycle)
    repo
  }
}

object ReactiveMongoApi {
  private var reactiveMongoApi: Option[DefaultReactiveMongoApi] = None

  def create(configuration: Configuration, applicationLifecycle: ApplicationLifecycle): ReactiveMongoApi = this.synchronized {
    reactiveMongoApi.getOrElse {
      val uri = MongoConnection.parseURI(configuration.getString("mongodb.uri").getOrElse(
        throw new AdaException("Cannot start Mongo. The configuration param 'mongo.uri' undefined.")
      )).getOrElse(
        throw new AdaException("The configuration param 'mongo.uri' cannot be parsed.")
      )

      val dbName = uri.db.getOrElse(configuration.getString("mongodb.db").getOrElse(
        throw new AdaException("The configuration param 'mongodb.db' undefined.")
      ))

      val strictUri = configuration.getBoolean("mongodb.connection.strictUri").getOrElse(true)

      reactiveMongoApi = Some(new DefaultReactiveMongoApi(uri, dbName, strictUri, configuration, applicationLifecycle))
      reactiveMongoApi.get
    }
  }

  def get = reactiveMongoApi
}