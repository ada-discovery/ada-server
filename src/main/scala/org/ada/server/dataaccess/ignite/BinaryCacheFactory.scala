package org.ada.server.dataaccess.ignite

import java.io.Serializable
import javax.cache.configuration.Factory
import javax.inject.Inject

import org.ada.server.dataaccess._
import org.ada.server.models.{FieldTypeId, FieldTypeSpec}
import FieldTypeId._
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.store.CacheStore
import org.apache.ignite.cache.{CacheAtomicityMode, CacheMode, QueryEntity, QueryIndex}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteCache}
import play.api.Logger
import play.api.libs.json.JsObject
import org.incal.core.dataaccess._

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import org.ada.server.dataaccess.ignite.BinaryJsonUtil.{escapeIgniteFieldName, getValueFromJson}
import org.ada.server.field.FieldTypeHelper

import scala.reflect.ClassTag
import scala.concurrent.ExecutionContext.Implicits.global

class BinaryCacheFactory @Inject()(ignite: Ignite) extends Serializable {

  private val ftf = FieldTypeHelper.fieldTypeFactory()

  def apply[ID](
    cacheName: String,
    fieldNamesAndTypes: Seq[(String, Class[_ >: Any])],
    repoFactory: Factory[AsyncCrudRepo[JsObject, ID]],
    getId: JsObject => Option[ID])(
    implicit tagId: ClassTag[ID]
  ): IgniteCache[ID, BinaryObject] = {
//    val fieldNameClassMap = createFieldNameClassMapFromDictionary(idFieldName, fieldNamesAndTypes)
    val cacheStoreFactory = new BinaryCacheCrudRepoStoreFactory[ID](cacheName, ignite, repoFactory, getId, fieldNamesAndTypes.toMap)
    apply(
      cacheName,
      fieldNamesAndTypes,
      Some(cacheStoreFactory)
    )
  }

  def apply[ID](
    cacheName: String,
    fieldNamesAndTypes: Seq[(String, Class[_])],
    cacheStoreFactoryOption: Option[Factory[CacheStore[ID, BinaryObject]]])(
    implicit tagId: ClassTag[ID]
  ): IgniteCache[ID, BinaryObject] = {
    val cacheConfig = new CacheConfiguration[ID, BinaryObject]()

    cacheConfig.setName(cacheName)
    cacheConfig.setCacheMode(CacheMode.LOCAL)
    cacheConfig.setAtomicityMode(CacheAtomicityMode.ATOMIC)

//    val fieldNameTypeMap = fieldNamesAndTypes match {
//      case _ => {
//        // if no fieldRepo provided fail over to a back-up plan and obtain the field types from the first row of the provided data set... this is however not recommended
//        logger.warn(s"No dictionary (field repo) provided for the JSON data cache '$cacheName'. Going to obtain the fields and types from the first row of the data set.")
//        Await.result(
//          createFieldNameTypeMapFromDataSet(idFieldName, repoFactory.create),
//          2 minutes
//        )
//      }
//      case _ =>
//        createFieldNameTypeMapFromDictionary(idFieldName, fieldNamesAndTypes)
//    }
//      fieldNameTypeMap.map{ case (fieldName, typeName) =>
//      (fieldName,
//        if (typeName.equals("scala.Enumeration.Value"))
//          classOf[String].asInstanceOf[Class[Any]]
//        else
//          Class.forName(typeName).asInstanceOf[Class[Any]])
//    }

    val fieldNameTypeNameMap = fieldNamesAndTypes.map{ case (fieldName, clazz) => (fieldName, clazz.getName)}.toMap

    val queryEntity = new QueryEntity() {
      setKeyType(tagId.runtimeClass.getName)
      setValueType(cacheName)
//      setIndexes(fieldNames.map(fieldName => new QueryIndex(fieldName)))
      setFields(new java.util.LinkedHashMap[String, String](fieldNameTypeNameMap))
    }

    cacheConfig.setQueryEntities(Seq(queryEntity))

    cacheStoreFactoryOption.foreach{ cacheStoreFactory =>
      cacheConfig.setCacheStoreFactory(cacheStoreFactory)
      cacheConfig.setWriteThrough(true)
      cacheConfig.setReadThrough(true)
    }

    ignite.getOrCreateCache(cacheConfig).withKeepBinary()
  }

  private def createFieldNameTypeMapFromDictionary[ID](
    idFieldName: String,
    fieldNamesAndTypes: Seq[(String, FieldTypeId.Value)])(
    implicit tagId: ClassTag[ID]
  ): Map[String, String] =
    (
      fieldNamesAndTypes.map{ case (fieldName, fieldType) =>
        (escapeIgniteFieldName(fieldName), ftf(FieldTypeSpec(fieldType)).valueClass.getName)
      } ++
        Seq((idFieldName, tagId.runtimeClass.getName))
    ).toMap

  private def createFieldNameClassMapFromDictionary[ID](
    idFieldName: String,
    fieldNamesAndTypes: Seq[(String, FieldTypeId.Value)])(
    implicit tagId: ClassTag[ID]
  ): Map[String, Class[_]] =
    (
      fieldNamesAndTypes.map{ case (fieldName, fieldType) =>
        (escapeIgniteFieldName(fieldName), ftf(FieldTypeSpec(fieldType)).valueClass.asInstanceOf[Class[_ >: Any]])
      } ++
        Seq((idFieldName, tagId.runtimeClass.asInstanceOf[Class[_ >: Any]]))
     ).toMap
}