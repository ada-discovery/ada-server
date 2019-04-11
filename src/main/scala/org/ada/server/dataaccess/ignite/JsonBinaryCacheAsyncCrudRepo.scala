package org.ada.server.dataaccess.ignite

import javax.inject.Inject

import org.ada.server.models.{DataSetFormattersAndIds, FieldTypeId, FieldTypeSpec}
import DataSetFormattersAndIds.JsObjectIdentity
import org.ada.server.dataaccess._
import org.ada.server.dataaccess.RepoTypes._
import org.ada.server.dataaccess.ignite.BinaryJsonUtil._
import org.ada.server.dataaccess.mongo.MongoJsonRepoFactory
import org.ada.server.field.FieldTypeFactory
import org.apache.ignite.{Ignite, IgniteCache}
import org.apache.ignite.binary.BinaryObject
import org.incal.core.Identity
import play.api.Configuration
import play.api.libs.json.JsObject
import reactivemongo.bson.BSONObjectID

class JsonBinaryCacheAsyncCrudRepo[ID](
    cache: IgniteCache[ID, BinaryObject],
    cacheName: String,
    val ignite: Ignite,
    identity: Identity[JsObject, ID]
  ) extends AbstractCacheAsyncCrudRepo[ID, JsObject, ID, BinaryObject](cache, cacheName, identity) {

  private val igniteBinary = ignite.binary
  private val toBinary = toBinaryObject(igniteBinary, fieldNameClassMap, cacheName)_

  // hooks
  override def toCacheId(id: ID) =
    id

  override def toItem(cacheItem: BinaryObject) =
    toJsObject(cacheItem)

  override def toCacheItem(item: JsObject) =
    toBinary(item)

  override def findResultToItem(result: Traversable[(String, Any)]) =
    toJsObject(result)
}

// protected[dataaccess]
class JsonBinaryCacheAsyncCrudRepoFactory @Inject()(
    ignite: Ignite,
    cacheFactory: BinaryCacheFactory,
    configuration: Configuration
  ) extends MongoJsonCrudRepoFactory {

  private val ftf = FieldTypeFactory(Set[String](""), Seq[String](), "", ",", true)

  override def apply(
    collectionName: String,
    fieldNamesAndTypes: Seq[(String, FieldTypeSpec)],
    createIndexForProjectionAutomatically: Boolean
  ) =
    applyWithDictionaryAux(collectionName, fieldNamesAndTypes, createIndexForProjectionAutomatically)

  private def applyWithDictionaryAux(
    collectionName: String,
    fieldNamesAndTypes: Seq[(String, FieldTypeSpec)],
    createIndexForProjectionAutomatically: Boolean
  ): JsonCrudRepo = {
    val cacheName = collectionName.replaceAll("[\\.-]", "_")
    val identity = JsObjectIdentity

    val fieldNamesAndClasses: Seq[(String, Class[_ >: Any])] =
      (fieldNamesAndTypes.map{ case (fieldName, fieldTypeSpec) =>
        (escapeIgniteFieldName(fieldName), ftf(fieldTypeSpec).valueClass.asInstanceOf[Class[_ >: Any]])
      } ++ Seq((identity.name, classOf[Option[BSONObjectID]].asInstanceOf[Class[_ >: Any]])))

    val cache = cacheFactory(
      cacheName,
      fieldNamesAndClasses,
      new MongoJsonRepoFactory(collectionName, createIndexForProjectionAutomatically, configuration, new SerializableApplicationLifecycle()),
      identity.of(_)
    ) // new DefaultApplicationLifecycle().addStopHook
    cache.loadCache(null)
    new JsonBinaryCacheAsyncCrudRepo(cache, cacheName, ignite, identity)
  }
}