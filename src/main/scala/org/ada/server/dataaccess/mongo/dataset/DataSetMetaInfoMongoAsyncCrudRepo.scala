package org.ada.server.dataaccess.mongo.dataset

import javax.cache.configuration.Factory
import javax.inject.Inject

import com.google.inject.assistedinject.Assisted
import org.ada.server.dataaccess.mongo.{MongoAsyncCrudExtraRepo, MongoAsyncCrudRepo, ReactiveMongoApi, SubordinateObjectMongoAsyncCrudRepo}
import org.ada.server.models.DataSetFormattersAndIds._
import org.ada.server.models._
import org.incal.core.dataaccess.AsyncCrudRepo
import play.api.Configuration
import play.api.inject.ApplicationLifecycle
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat

import scala.concurrent.Future

class DataSetMetaInfoMongoAsyncCrudRepoFactory(
    dataSpaceId: BSONObjectID,
    configuration: Configuration,
    applicationLifecycle: ApplicationLifecycle
  ) extends Factory[AsyncCrudRepo[DataSetMetaInfo, BSONObjectID]] {

  override def create(): AsyncCrudRepo[DataSetMetaInfo, BSONObjectID] = {
    val dataSpaceRepo = new MongoAsyncCrudRepo[DataSpaceMetaInfo, BSONObjectID]("dataspace_meta_infos")
    dataSpaceRepo.reactiveMongoApi = ReactiveMongoApi.create(configuration, applicationLifecycle)

    val repo = new DataSetMetaInfoMongoAsyncCrudRepo(dataSpaceId, dataSpaceRepo)
    repo.initIfNeeded
    repo
  }
}

class DataSetMetaInfoMongoAsyncCrudRepo @Inject()(
    @Assisted dataSpaceId: BSONObjectID,
    dataSpaceMetaInfoRepo: MongoAsyncCrudExtraRepo[DataSpaceMetaInfo, BSONObjectID]
  ) extends SubordinateObjectMongoAsyncCrudRepo[DataSetMetaInfo, BSONObjectID, DataSpaceMetaInfo, BSONObjectID]("dataSetMetaInfos", dataSpaceMetaInfoRepo) {

  rootId = Some(Future(dataSpaceId)) // initialize the data space id

  override protected def getDefaultRoot =
    DataSpaceMetaInfo(Some(dataSpaceId), "", 0, new java.util.Date(), Seq[DataSetMetaInfo]())

  override protected def getRootObject =
    Future(Some(getDefaultRoot))
  //    rootRepo.find(Seq(DataSpaceMetaInfoIdentity.name #== dataSpaceId)).map(_.headOption)

  override def save(entity: DataSetMetaInfo): Future[BSONObjectID] = {
    val identity = DataSetMetaInfoIdentity
    val initializedId = identity.of(entity).getOrElse(BSONObjectID.generate)
    super.save(identity.set(entity, initializedId))
  }
}