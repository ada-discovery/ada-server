package org.ada.server.dataaccess.ignite

import java.io.Serializable
import java.{util => ju}
import javax.cache.Cache.Entry
import javax.cache.configuration.Factory

import org.incal.core.dataaccess._
import org.apache.ignite.Ignite
import org.apache.ignite.binary.{BinaryObject, BinaryType}
import org.apache.ignite.cache.store.{CacheStore, CacheStoreAdapter}
import org.apache.ignite.lang.IgniteBiInClosure
import org.apache.ignite.resources.IgniteInstanceResource
import play.api.Logger
import play.api.libs.json.{JsObject, Json, JsValue}
import scala.collection.JavaConversions._
import org.ada.server.dataaccess.ignite.BinaryJsonUtil._

import scala.concurrent.duration._

protected class BinaryCacheCrudRepoStoreAdapter[ID](
    cacheName: String,
    val repoFactory: Factory[AsyncCrudRepo[JsObject, ID]],
    val getId: JsObject => Option[ID],
    fieldNameClassMap: Map[String, Class[_ >: Any]]
  ) extends AbstractCacheAsyncCrudRepoProvider[ID, BinaryObject, JsObject] with Serializable {

  @IgniteInstanceResource
  private var ignite: Ignite = _
  private lazy val toBinary = toBinaryObject(ignite.binary(), fieldNameClassMap, cacheName)_

  override def toRepoItem = toJsObject(_)
  override def fromRepoItem = toBinary
}

protected class BinaryCacheCrudRepoStoreFactory[ID](
    cacheName: String,
    ignite: Ignite,
    repoFactory: Factory[AsyncCrudRepo[JsObject, ID]],
    getId: JsObject => Option[ID],
    fieldNameClassMap: Map[String, Class[_ >: Any]]
  ) extends Factory[CacheStore[ID, BinaryObject]] {

  override def create(): CacheStore[ID, BinaryObject] =
    new BinaryCacheCrudRepoStoreAdapter[ID](cacheName, repoFactory, getId, fieldNameClassMap)
}