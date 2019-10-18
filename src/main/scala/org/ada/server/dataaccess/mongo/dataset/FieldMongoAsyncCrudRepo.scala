package org.ada.server.dataaccess.mongo.dataset

import javax.cache.configuration.Factory
import javax.inject.Inject

import com.google.inject.assistedinject.Assisted
import org.ada.server.models.{DataSetFormattersAndIds, Dictionary, Field}
import DataSetFormattersAndIds._
import org.ada.server.dataaccess.mongo.{MongoAsyncCrudRepo, ReactiveMongoApi}
import org.ada.server.dataaccess.RepoTypes.DictionaryRootRepo
import org.incal.core.dataaccess.AsyncCrudRepo
import play.api.Configuration
import play.api.inject.ApplicationLifecycle
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat

class FieldMongoAsyncCrudRepo @Inject()(
    @Assisted dataSetId : String,
    dictionaryRepo: DictionaryRootRepo
  ) extends DictionarySubordinateMongoAsyncCrudRepo[Field, String]("fields", dataSetId, dictionaryRepo)

class FieldMongoAsyncCrudRepoFactory(
    dataSetId: String,
    configuration: Configuration,
    applicationLifecycle: ApplicationLifecycle
  ) extends Factory[AsyncCrudRepo[Field, String]] {

  override def create(): AsyncCrudRepo[Field, String] = {
    val dictionaryRepo = new MongoAsyncCrudRepo[Dictionary, BSONObjectID]("dictionaries")
    dictionaryRepo.reactiveMongoApi = ReactiveMongoApi.create(configuration, applicationLifecycle)

    val repo = new FieldMongoAsyncCrudRepo(dataSetId, dictionaryRepo)
    repo.initIfNeeded
    repo
  }
}

