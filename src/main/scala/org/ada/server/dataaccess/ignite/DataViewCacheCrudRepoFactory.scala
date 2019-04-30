package org.ada.server.dataaccess.ignite

import javax.inject.Inject
import org.ada.server.dataaccess.RepoTypes.DataViewRepo
import org.ada.server.dataaccess._
import org.ada.server.dataaccess.dataset.DataViewRepoFactory
import org.ada.server.dataaccess.mongo.dataset.DataViewMongoAsyncCrudRepoFactory
import org.ada.server.models.DataView.DataViewIdentity
import play.api.Configuration

protected[dataaccess] class DataViewCacheCrudRepoFactory @Inject()(
    cacheRepoFactory: CacheAsyncCrudRepoFactory,
    configuration: Configuration
  ) extends DataViewRepoFactory {

  def apply(dataSetId: String): DataViewRepo = {
    val cacheName = "DataView_" + dataSetId.replaceAll("[\\.-]", "_")
    val mongoRepoFactory = new DataViewMongoAsyncCrudRepoFactory(dataSetId, configuration, new SerializableApplicationLifecycle())
    cacheRepoFactory(mongoRepoFactory, cacheName)
  }
}