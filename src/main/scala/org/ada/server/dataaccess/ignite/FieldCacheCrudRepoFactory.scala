package org.ada.server.dataaccess.ignite

import javax.inject.Inject
import org.ada.server.models.DataSetFormattersAndIds
import DataSetFormattersAndIds.FieldIdentity
import org.ada.server.dataaccess.RepoTypes.FieldRepo
import org.ada.server.dataaccess._
import org.ada.server.dataaccess.dataset.FieldRepoFactory
import org.ada.server.dataaccess.mongo.dataset.FieldMongoAsyncCrudRepoFactory
import play.api.Configuration

protected[dataaccess] class FieldCacheCrudRepoFactory @Inject()(
    cacheRepoFactory: CacheAsyncCrudRepoFactory,
    configuration: Configuration
  ) extends FieldRepoFactory {

  def apply(dataSetId: String): FieldRepo = {
    val cacheName = "Field_" + dataSetId.replaceAll("[\\.-]", "_")
    val mongoRepoFactory = new FieldMongoAsyncCrudRepoFactory(dataSetId, configuration, new SerializableApplicationLifecycle())
    cacheRepoFactory(mongoRepoFactory, cacheName, Set("enumValues"))
  }
}