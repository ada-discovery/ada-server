package org.ada.server.dataaccess.ignite

import javax.inject.Inject

import org.ada.server.dataaccess.RepoTypes.DataSetMetaInfoRepo
import org.ada.server.dataaccess._
import org.ada.server.dataaccess.mongo.dataset.DataSetMetaInfoMongoAsyncCrudRepoFactory
import play.api.Configuration
import reactivemongo.bson.BSONObjectID
import org.ada.server.models.DataSetFormattersAndIds.DataSetMetaInfoIdentity

class DataSetMetaInfoCacheCrudRepoFactory @Inject()(
    cacheRepoFactory: CacheAsyncCrudRepoFactory,
    configuration: Configuration
  ) extends DataSetMetaInfoRepoFactory {

  def apply(dataSpaceId: BSONObjectID): DataSetMetaInfoRepo = {
    val cacheName = "DataSetMetaInfo_" + dataSpaceId.stringify
    val mongoRepoFactory = new DataSetMetaInfoMongoAsyncCrudRepoFactory(dataSpaceId, configuration, new SerializableApplicationLifecycle())
    cacheRepoFactory(mongoRepoFactory, cacheName)
  }
}