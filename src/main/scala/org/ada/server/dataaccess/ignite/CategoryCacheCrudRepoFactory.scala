package org.ada.server.dataaccess.ignite

import javax.inject.Inject
import org.ada.server.models.DataSetFormattersAndIds
import DataSetFormattersAndIds.CategoryIdentity
import org.ada.server.dataaccess.RepoTypes.CategoryRepo
import org.ada.server.dataaccess._
import org.ada.server.dataaccess.dataset.CategoryRepoFactory
import org.ada.server.dataaccess.mongo.dataset.CategoryMongoAsyncCrudRepoFactory
import play.api.Configuration

protected[dataaccess] class CategoryCacheCrudRepoFactory @Inject()(
    cacheRepoFactory: CacheAsyncCrudRepoFactory,
    configuration: Configuration
  ) extends CategoryRepoFactory {

  def apply(dataSetId: String): CategoryRepo = {
    val cacheName = "Category_" + dataSetId.replaceAll("[\\.-]", "_")
    val mongoRepoFactory = new CategoryMongoAsyncCrudRepoFactory(dataSetId, configuration, new SerializableApplicationLifecycle())
    cacheRepoFactory(mongoRepoFactory, cacheName)
  }
}