package org.ada.server.dataaccess.ignite

import java.io.Serializable
import java.util
import javax.cache.Cache.Entry
import javax.cache.configuration.Factory

import org.apache.ignite.cache.store.{CacheStore, CacheStoreAdapter}
import org.apache.ignite.lang.IgniteBiInClosure
import org.incal.core.dataaccess.{AsyncCrudRepo, RepoSynchronizer}
import play.api.Logger

import scala.concurrent.duration._
import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

//@CacheLocalStore
abstract class AbstractCacheAsyncCrudRepoProvider[ID, E, REPO_E] extends CacheStoreAdapter[ID, E] with Serializable {

  val repoFactory: Factory[AsyncCrudRepo[REPO_E, ID]]
  def toRepoItem: E => REPO_E
  def fromRepoItem: REPO_E => E
  def getId: REPO_E => Option[ID]

  private val logger = Logger
  private val crudRepo: AsyncCrudRepo[REPO_E, ID] = repoFactory.create
  private lazy val syncRepo = RepoSynchronizer(crudRepo, 2 minutes)

  override def delete(key: Any) =
    syncRepo.delete(key.asInstanceOf[ID])

  override def deleteAll(keys: util.Collection[_]) =
    syncRepo.delete(keys.map(_.asInstanceOf[ID]))

  override def write(entry: Entry[_ <: ID, _ <: E]): Unit = {
    val id = entry.getKey
    val item = toRepoItem(entry.getValue)
//    val version = entry.getVersion

    // TODO: replace with a single upsert (save/update) call
    //      syncRepo.get(id).map { _ =>
    val future = for {
      exists <- crudRepo.exists(id)
      _ <- if (exists) {
        logger.info(s"Updating an item of type ${item.getClass.getSimpleName}")
        crudRepo.update(item)
      } else {
        logger.info(s"Saving an item of type ${item.getClass.getSimpleName}")
        crudRepo.save(item)
      }
    } yield
      ()

    Await.result(future, 2 minutes)
  }

  override def writeAll(entries: util.Collection[Entry[_ <: ID, _ <: E]]): Unit = {
    val ids = entries.map(_.getKey)
    val items = entries.map(entry => toRepoItem(entry.getValue))

    if (items.nonEmpty) {
      // TODO: save vs update
      logger.info(s"Saving ${items.size} items of type ${items.head.getClass.getSimpleName}")
      syncRepo.save(items)
    }
  }

  override def load(key: ID): E = {
    logger.info(s"Loading item for key of type ${key.getClass.getSimpleName}")
    syncRepo.get(key).map(fromRepoItem).getOrElse(null.asInstanceOf[E])
  }

  override def loadCache(clo: IgniteBiInClosure[ID, E], args: AnyRef *) = {
    logger.info("Loading Cache")
    syncRepo.find().foreach( item =>
      getId(item).map(clo.apply(_, fromRepoItem(item)))
    )
  }

  //  override def loadAll() = {
}