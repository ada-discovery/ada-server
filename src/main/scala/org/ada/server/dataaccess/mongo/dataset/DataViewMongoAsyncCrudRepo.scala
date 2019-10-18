package org.ada.server.dataaccess.mongo.dataset

import javax.cache.configuration.Factory
import javax.inject.Inject

import com.google.inject.assistedinject.Assisted
import org.ada.server.dataaccess.mongo.{MongoAsyncCrudRepo, ReactiveMongoApi}
import org.ada.server.dataaccess.RepoTypes.DictionaryRootRepo
import org.ada.server.models.{DataView, Dictionary}
import org.ada.server.models.DataSetFormattersAndIds.{DictionaryIdentity, dictionaryFormat}
import org.ada.server.models.DataView.{DataViewIdentity, dataViewFormat}
import org.incal.core.dataaccess.AsyncCrudRepo
import play.api.Configuration
import play.api.inject.ApplicationLifecycle
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat

import scala.concurrent.Future

class DataViewMongoAsyncCrudRepo @Inject()(
    @Assisted dataSetId : String,
    dictionaryRepo: DictionaryRootRepo
  ) extends DictionarySubordinateMongoAsyncCrudRepo[DataView, BSONObjectID]("dataviews", dataSetId, dictionaryRepo) {

  override def save(entity: DataView): Future[BSONObjectID] = {
    val initializedId = DataViewIdentity.of(entity).getOrElse(BSONObjectID.generate)
    super.save(DataViewIdentity.set(entity, initializedId))
  }
}

class DataViewMongoAsyncCrudRepoFactory(
    dataSetId: String,
    configuration: Configuration,
    applicationLifecycle: ApplicationLifecycle
  ) extends Factory[AsyncCrudRepo[DataView, BSONObjectID]] {

  override def create(): AsyncCrudRepo[DataView, BSONObjectID] = {
    val dictionaryRepo = new MongoAsyncCrudRepo[Dictionary, BSONObjectID]("dictionaries")
    dictionaryRepo.reactiveMongoApi = ReactiveMongoApi.create(configuration, applicationLifecycle)

    val repo = new DataViewMongoAsyncCrudRepo(dataSetId, dictionaryRepo)
    repo.initIfNeeded
    repo
  }
}