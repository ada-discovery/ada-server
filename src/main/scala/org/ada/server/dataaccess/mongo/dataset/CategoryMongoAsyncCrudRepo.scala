package org.ada.server.dataaccess.mongo.dataset

import javax.cache.configuration.Factory
import javax.inject.Inject

import com.google.inject.assistedinject.Assisted
import org.ada.server.dataaccess.mongo.{ReactiveMongoApi, MongoAsyncCrudRepo}
import org.ada.server.dataaccess.RepoTypes.DictionaryRootRepo
import org.ada.server.models.{Category, Dictionary, DataSetFormattersAndIds}
import DataSetFormattersAndIds.{CategoryIdentity, categoryFormat, dictionaryFormat, DictionaryIdentity}
import play.api.Configuration
import play.api.inject.ApplicationLifecycle
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat
import org.incal.core.dataaccess.AsyncCrudRepo

import scala.concurrent.Future

class CategoryMongoAsyncCrudRepo @Inject()(
    @Assisted dataSetId : String,
    dictionaryRepo: DictionaryRootRepo
  ) extends DictionarySubordinateMongoAsyncCrudRepo[Category, BSONObjectID]("categories", dataSetId, dictionaryRepo) {

  override def save(entity: Category): Future[BSONObjectID] = {
    val initializedId = CategoryIdentity.of(entity).getOrElse(BSONObjectID.generate)
    super.save(CategoryIdentity.set(entity, initializedId))
  }
}

class CategoryMongoAsyncCrudRepoFactory(
    dataSetId: String,
    configuration: Configuration,
    applicationLifecycle: ApplicationLifecycle
  ) extends Factory[AsyncCrudRepo[Category, BSONObjectID]] {

  override def create(): AsyncCrudRepo[Category, BSONObjectID] = {
    val dictionaryRepo = new MongoAsyncCrudRepo[Dictionary, BSONObjectID]("dictionaries")
    dictionaryRepo.reactiveMongoApi = ReactiveMongoApi.create(configuration, applicationLifecycle)

    val repo = new CategoryMongoAsyncCrudRepo(dataSetId, dictionaryRepo)
    repo.initIfNeeded
    repo
  }
}