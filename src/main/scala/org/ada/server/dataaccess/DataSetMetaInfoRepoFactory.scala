package org.ada.server.dataaccess

import com.google.inject.ImplementedBy
import org.ada.server.dataaccess.RepoTypes.DataSetMetaInfoRepo
import org.ada.server.dataaccess.ignite.DataSetMetaInfoCacheCrudRepoFactory
import reactivemongo.bson.BSONObjectID

@ImplementedBy(classOf[DataSetMetaInfoCacheCrudRepoFactory])
trait DataSetMetaInfoRepoFactory {
  def apply(dataSpaceId: BSONObjectID): DataSetMetaInfoRepo
}
