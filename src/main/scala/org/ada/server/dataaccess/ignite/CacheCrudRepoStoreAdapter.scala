package org.ada.server.dataaccess.ignite

import java.io.Serializable
import javax.cache.configuration.Factory

import org.apache.ignite.cache.store.CacheStore
import org.incal.core.dataaccess.AsyncCrudRepo

protected class CacheCrudRepoStoreAdapter[ID, E](
    val repoFactory: Factory[AsyncCrudRepo[E, ID]],
    val getId: E => Option[ID]
  ) extends AbstractCacheAsyncCrudRepoProvider[ID, E, E] with Serializable {

  override def toRepoItem = identity[E]
  override def fromRepoItem = identity[E]
}

protected class CacheCrudRepoStoreFactory[ID, E](
    repoFactory: Factory[AsyncCrudRepo[E, ID]],
    getId: E => Option[ID]
  ) extends Factory[CacheStore[ID, E]] {

  override def create(): CacheStore[ID, E] =
    new CacheCrudRepoStoreAdapter[ID, E](repoFactory, getId)
}