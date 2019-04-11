package org.ada.server.dataaccess.mongo

import javax.cache.configuration.Factory
import com.google.inject.assistedinject.Assisted
import org.ada.server.models.FieldTypeSpec
import play.api.libs.json.{JsObject, Json}
import play.api.Configuration
import play.api.inject.ApplicationLifecycle
import play.modules.reactivemongo.{DefaultReactiveMongoApi, ReactiveMongoApi}
import reactivemongo.bson.BSONObjectID
import play.modules.reactivemongo.json._
import org.incal.core.dataaccess.{AsyncCrudRepo, InCalDataAccessException}
import org.ada.server.dataaccess.RepoTypes.JsonCrudRepo
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.modules.reactivemongo.json.BSONFormats.BSONObjectIDFormat

import scala.concurrent.Future
import javax.inject.Inject
import org.ada.server.models.DataSetFormattersAndIds.JsObjectIdentity
import reactivemongo.core.commands.RawCommand

class MongoJsonCrudRepo @Inject()(
    @Assisted collectionName : String,
    @Assisted fieldNamesAndTypes: Seq[(String, FieldTypeSpec)],
    @Assisted mongoAutoCreateIndexForProjection: Boolean
  ) extends MongoAsyncReadonlyRepo[JsObject, BSONObjectID](collectionName, JsObjectIdentity.name, mongoAutoCreateIndexForProjection) with JsonCrudRepo {

  override def save(entity: JsObject): Future[BSONObjectID] = {
    val (doc, id) = addId(entity)

    collection.insert(entity).map {
      case le if le.ok => id
      case le => throw new InCalDataAccessException(le.writeErrors.map(_.errmsg).mkString(". "))
    }
  }

  override def save(entities: Traversable[JsObject]): Future[Traversable[BSONObjectID]] = {
    val docAndIds = entities.map(addId)

    collection.bulkInsert(docAndIds.map(_._1).toStream, ordered = false).map {
      case le if le.ok => docAndIds.map(_._2)
      case le => throw new InCalDataAccessException(le.errmsg.getOrElse(""))
    }
  }

  private def addId(entity: JsObject): (JsObject, BSONObjectID) = {
    val id = BSONObjectID.generate
    (entity ++ Json.obj(identityName -> id), id)
  }

  override def update(entity: JsObject): Future[BSONObjectID] = {
    val id = (entity \ identityName).as[BSONObjectID]
    collection.update(Json.obj(identityName -> id), entity) map {
      case le if le.ok => id
      case le => throw new InCalDataAccessException(le.writeErrors.map(_.errmsg).mkString(". "))
    }
  }

  override def delete(id: BSONObjectID) =
    collection.remove(Json.obj(identityName -> id)) map handleResult

  override def deleteAll : Future[Unit] =
    collection.remove(Json.obj()) map handleResult

  override def flushOps = {
    val rawCommand = RawCommand(FSyncCommand().toBSON)
    reactiveMongoApi.db.command(rawCommand).map(_ => ())
    //    collection.runCommand(FSyncCommand()).map(_ => ())
  }
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

  def create(configuration: Configuration, applicationLifecycle: ApplicationLifecycle): ReactiveMongoApi = {
    reactiveMongoApi.getOrElse {
      reactiveMongoApi = Some(new DefaultReactiveMongoApi(configuration, applicationLifecycle))
      reactiveMongoApi.get
    }
  }

  def get = reactiveMongoApi
}