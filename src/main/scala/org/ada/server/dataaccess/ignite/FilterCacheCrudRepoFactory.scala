package org.ada.server.dataaccess.ignite

import javax.inject.Inject
import org.ada.server.dataaccess.RepoTypes.FilterRepo
import org.ada.server.dataaccess._
import org.ada.server.dataaccess.dataset.FilterRepoFactory
import org.ada.server.dataaccess.mongo.dataset.FilterMongoAsyncCrudRepoFactory
import org.ada.server.models.Filter.FilterIdentity
import play.api.Configuration

protected[dataaccess] class FilterCacheCrudRepoFactory @Inject()(
    cacheRepoFactory: CacheAsyncCrudRepoFactory,
    configuration: Configuration
  ) extends FilterRepoFactory {

  def apply(dataSetId: String): FilterRepo = {
    val cacheName = "Filter_" + dataSetId.replaceAll("[\\.-]", "_")
    val mongoRepoFactory = new FilterMongoAsyncCrudRepoFactory(dataSetId, configuration, new SerializableApplicationLifecycle())
    cacheRepoFactory(mongoRepoFactory, cacheName)
  }
}