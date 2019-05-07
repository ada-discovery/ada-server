package org.ada.server.dataaccess.ignite

import org.apache.ignite.cache.store.CacheStore

import scala.reflect.runtime.universe._
import java.io.Serializable
import javax.cache.configuration.Factory
import javax.inject.Inject

import org.incal.core.dataaccess.AsyncCrudRepo
import org.incal.core.util.ReflectionUtil.getCaseClassMemberAndTypeNames
import org.apache.ignite.cache._
import org.apache.ignite.configuration.{BinaryConfiguration, CacheConfiguration}
import org.apache.ignite.{Ignite, IgniteCache}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class CacheFactory @Inject()(ignite: Ignite) extends Serializable {

  private val writeThrough = true
  private val readThrough = true

  def apply[ID, E](
    cacheName: String,
    repoFactory: Factory[AsyncCrudRepo[E, ID]],
    getId: E => Option[ID],
    fieldsToExcludeFromIndex: Set[String])(
    implicit tagId: ClassTag[ID], typeTagE: TypeTag[E]
  ): IgniteCache[ID, E] =
    apply(
      cacheName,
      Some(new CacheCrudRepoStoreFactory[ID, E](repoFactory, getId)),
      fieldsToExcludeFromIndex
    )

  def apply[ID, E](
    cacheName: String,
    cacheStoreFactoryOption: Option[Factory[CacheStore[ID, E]]],
    fieldsToExcludeFromIndex: Set[String])(
    implicit tagId: ClassTag[ID], typeTagE: TypeTag[E]
  ): IgniteCache[ID, E] = {
    val cacheConfig = new CacheConfiguration[ID, E]()

    val fieldNamesAndTypes = getCaseClassMemberAndTypeNames[E]
    val fieldNames = fieldNamesAndTypes.map(_._1)
    val fields = fieldNamesAndTypes.toMap
    val indeces = fieldNames.filterNot(fieldsToExcludeFromIndex.contains).map(new QueryIndex(_)).toSeq

    val queryEntity = new QueryEntity() {
      setKeyType(tagId.runtimeClass.getName)
      setValueType(typeOf[E].typeSymbol.fullName)
      setFields(new java.util.LinkedHashMap[String, String](fields))
      setIndexes(indeces)
    }

    cacheConfig.setSqlFunctionClasses(classOf[CustomSqlFunctions])
    cacheConfig.setName(cacheName)
    cacheConfig.setQueryEntities(Seq(queryEntity))
    cacheConfig.setCacheMode(CacheMode.LOCAL) // PARTITIONED
    cacheConfig.setAtomicityMode(CacheAtomicityMode.ATOMIC)
    cacheConfig.setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.PRIMARY)

    cacheStoreFactoryOption.foreach{ cacheStoreFactory =>
      cacheConfig.setCacheStoreFactory(cacheStoreFactory)
      cacheConfig.setWriteThrough(writeThrough)
//      cacheConfig.setWriteBehindEnabled(!writeThrough)
//      cacheConfig.setWriteBehindBatchSize()
      cacheConfig.setReadThrough(readThrough)
    }

//    val bCfg = new BinaryConfiguration()
//    bCfg.setIdMapper(new BinaryBasicIdMapper)
//    bCfg.setTypeConfigurations(util.Arrays.asList(new BinaryTypeConfiguration("org.my.Class")))

    ignite.getOrCreateCache(cacheConfig) // .withKeepBinary()
  }
}