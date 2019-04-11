package org.ada.server.dataaccess.ignite

import javax.cache.configuration.Factory
import javax.inject.Inject

import org.ada.server.dataaccess.mongo.{MongoAsyncCrudRepo, ReactiveMongoApi}
import org.ada.server.dataaccess._
import org.ada.server.dataaccess.ignite.BinaryJsonUtil.unescapeFieldName
import org.incal.core.util.DynamicConstructorFinder
import org.apache.ignite.{Ignite, IgniteCache}
import org.incal.core.Identity
import org.incal.core.util.ReflectionUtil.shortName
import org.incal.core.dataaccess._
import play.api.Configuration
import play.api.inject.ApplicationLifecycle
import play.api.libs.json.{Format, JsResult, JsValue}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import scala.reflect.ClassTag

private class CacheAsyncCrudRepo[ID, E: TypeTag](
    cache: IgniteCache[ID, E],
    entityName: String,
    val ignite: Ignite,
    identity: Identity[E, ID]
  ) extends AbstractCacheAsyncCrudRepo[ID, E, ID, E](cache, entityName, identity) {

  private val constructorFinder = DynamicConstructorFinder.apply[E]

  // hooks
  override def toCacheId(id: ID) =
    id

  override def toItem(cacheItem: E) =
//    cacheItem.asInstanceOf[BinaryObject].deserialize[E]
    cacheItem

  override def toCacheItem(item: E) =
    item

  override def findResultToItem(result: Traversable[(String, Any)]): E = {
    val fieldNameValueMap = result.map{ case (fieldName, value) => (unescapeFieldName(fieldName), value) }.toMap
    val fieldNames = fieldNameValueMap.map(_._1).toSeq

    val constructor = constructorOrException(fieldNames)

    constructor(fieldNameValueMap).get
  }

  override def findResultsToItems(rawFieldNames: Seq[String], results: Traversable[Seq[Any]]): Traversable[E] = {
    val fieldNames = rawFieldNames.map(unescapeFieldName)

    val constructor = constructorOrException(fieldNames)

    results.map { result =>
      // TODO: which one is faster? First or second constructor call?
      constructor(fieldNames.zip(result).toMap).get
//      constructor(result).get
    }
  }

  private def constructorOrException(fieldNames: Seq[String]) =
    constructorFinder(fieldNames).getOrElse{
      throw new IllegalArgumentException(s"No constructor of the class '${constructorFinder.classSymbol.fullName}' matches the query result fields '${fieldNames.mkString(", ")}'. Adjust your query or introduce an appropriate constructor.")
    }
}

class CacheAsyncCrudRepoFactory {

  @Inject var cacheFactory: CacheFactory = _
  @Inject var configuration: Configuration = _
  @Inject var ignite: Ignite = _

  private val applicationLifecycle = new SerializableApplicationLifecycle()

  def apply[ID: ClassTag, E: TypeTag](
    repoFactory: Factory[AsyncCrudRepo[E, ID]],
    cacheName: String,
    fieldsToExcludeFromIndex: Set[String] = Set())(
    implicit identity: Identity[E, ID]
  ): AsyncCrudRepo[E, ID] = {
    val cache = cacheFactory[ID, E](
      cacheName,
      repoFactory,
      identity.of(_),
      fieldsToExcludeFromIndex
    )
    cache.loadCache(null)
    val entityName = shortName(typeOf[E].typeSymbol)
    new CacheAsyncCrudRepo(cache, entityName, ignite, identity)
  }

  // Important: instead of passing Format with need to decompose (accept) only reads and writes functions
  def applyMongo[ID: ClassTag, E: TypeTag](
    mongoCollectionName: String,
    cacheName: Option[String] = None,
    fieldsToExcludeFromIndex: Set[String] = Set())(
    implicit formatId: Format[ID], formatE: Format[E], identity: Identity[E, ID]
  ): AsyncCrudRepo[E, ID] = {
    val repoFactory = new MongoAsyncCrudRepoFactory[E, ID](mongoCollectionName, configuration, applicationLifecycle)
    apply(repoFactory, cacheName.getOrElse(mongoCollectionName), fieldsToExcludeFromIndex)
  }
}

private class MongoAsyncCrudRepoFactory[E, ID](
    collectionName: String,
    configuration: Configuration,
    applicationLifecycle: ApplicationLifecycle)(
    implicit formatId: Format[ID], formatE: Format[E], identity: Identity[E, ID]
  ) extends Factory[AsyncCrudRepo[E, ID]] {

  override def create(): AsyncCrudRepo[E, ID] = {
    val repo = new MongoAsyncCrudRepo[E, ID](collectionName)
    repo.reactiveMongoApi = ReactiveMongoApi.create(configuration, applicationLifecycle)
    repo
  }
}