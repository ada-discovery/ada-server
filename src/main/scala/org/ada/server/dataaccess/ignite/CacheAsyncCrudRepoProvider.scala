package org.ada.server.dataaccess.ignite

import javax.inject.{Inject, Provider}

import org.incal.core.Identity
import org.incal.core.dataaccess.AsyncCrudRepo
import play.api.libs.json.Format

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

class CacheAsyncCrudRepoProvider[E: TypeTag, ID: ClassTag](
    val mongoCollectionName: String,
    val cacheName: Option[String] = None)(
    implicit val formatId: Format[ID], val formatE: Format[E], val identity: Identity[E, ID]
  ) extends Provider[AsyncCrudRepo[E, ID]] {

  @Inject var cacheRepoFactory: CacheAsyncCrudRepoFactory = _

  override def get(): AsyncCrudRepo[E, ID] =
    cacheRepoFactory.applyMongo[ID, E](mongoCollectionName, cacheName)
}