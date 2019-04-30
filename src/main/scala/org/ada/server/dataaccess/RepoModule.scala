package org.ada.server.dataaccess

import javax.inject.Provider
import com.google.inject.{Key, TypeLiteral}
import com.google.inject.assistedinject.FactoryModuleBuilder
import com.sksamuel.elastic4s.ElasticClient
import org.ada.server.dataaccess.elastic.{ElasticJsonCrudRepo, PlayElasticClientProvider}
import org.ada.server.dataaccess.ignite.{CacheAsyncCrudRepoProvider, JsonBinaryCacheAsyncCrudRepoFactory}
import org.incal.spark_ml.models.classification.Classifier
import org.incal.spark_ml.models.regression.Regressor
import org.ada.server.dataaccess._
import org.ada.server.dataaccess.mongo._
import org.ada.server.models.DataSetFormattersAndIds._
import org.ada.server.models._
import org.ada.server.models.ml.regression.Regressor._
import org.ada.server.models.ml.classification.Classifier._
import net.codingwell.scalaguice.ScalaModule
import org.ada.server.dataaccess.RepoTypes._
import org.ada.server.dataaccess.RepoTypes._
import com.google.inject.name.Names
import org.ada.server.models.dataimport.DataSetImport
import org.ada.server.models.{Message, Translation}
import org.ada.server.models.ml.unsupervised.UnsupervisedLearning
import org.ada.server.models.ml.unsupervised.UnsupervisedLearning.unsupervisedLearningFormat
import org.ada.server.dataaccess.dataset._
import reactivemongo.bson.BSONObjectID
import org.ada.server.dataaccess.RepoDef.Repo

private object RepoDef extends Enumeration {
  abstract class AbstractRepo[T: Manifest] extends super.Val {
    val named: Boolean
    val man: Manifest[T] = manifest[T]
  }

  case class Repo[T: Manifest](repo: T, named: Boolean = false) extends AbstractRepo[T]
  case class ProviderRepo[T: Manifest](provider: Provider[T], named: Boolean = false) extends AbstractRepo[T]

  implicit def valueToRepo[T](x: Value) = x.asInstanceOf[Repo[T]]

  import org.ada.server.models.dataimport.DataSetImport.{DataSetImportIdentity, dataSetImportFormat}
  import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat
  import org.ada.server.models.DataSetFormattersAndIds.{dataSetSettingFormat, fieldFormat, dictionaryFormat, DataSpaceMetaInfoIdentity, DictionaryIdentity, FieldIdentity, DataSetSettingIdentity}

  val TranslationRepo = Repo[TranslationRepo](
    new MongoAsyncCrudRepo[Translation, BSONObjectID]("translations"))

  val MessageRepo = Repo[MessageRepo](
    new MongoAsyncStreamRepo[Message, BSONObjectID]("messages", Some("timeCreated")))

  val ClassificationRepo = Repo[ClassifierRepo](
    new MongoAsyncCrudRepo[Classifier, BSONObjectID]("classifications"))

  val RegressionRepo = Repo[RegressorRepo](
    new MongoAsyncCrudRepo[Regressor, BSONObjectID]("regressions"))

  val UnsupervisedLearningRepo = Repo[UnsupervisedLearningRepo](
    new MongoAsyncCrudRepo[UnsupervisedLearning, BSONObjectID]("unsupervisedLearnings"))

  val DictionaryRootRepo = Repo[DictionaryRootRepo](
    new MongoAsyncCrudRepo[Dictionary, BSONObjectID]("dictionaries"))

//  val MongoDataSpaceMetaInfoRepo = Repo[MongoAsyncCrudExtraRepo[DataSpaceMetaInfo, BSONObjectID]](
//    new MongoAsyncCrudRepo[DataSpaceMetaInfo, BSONObjectID]("dataspace_meta_infos"), true)

  //  val DataSpaceMetaInfoRepo = Repo[MongoAsyncCrudExtraRepo[DataSpaceMetaInfo, BSONObjectID]](
//    new MongoAsyncCrudRepo[DataSpaceMetaInfo, BSONObjectID]("dataspace_meta_infos"))

//  val DataSetSettingRepo = Repo[DataSetSettingRepo](
//    new MongoAsyncCrudRepo[DataSetSetting, BSONObjectID]("dataset_settings"))

//  val DataSetImportRepo = Repo[DataSetImportRepo](
//    new ElasticFormatAsyncCrudRepo[DataSetImport, BSONObjectID]("dataset_imports", "dataset_imports", true, true, true, true))

  val DataSetImportRepo = Repo[DataSetImportRepo](
    new MongoAsyncCrudRepo[DataSetImport, BSONObjectID]("dataset_imports"))
}

// repo module used to bind repo types/instances withing Guice IoC container
class RepoModule extends ScalaModule {

  import org.ada.server.models.DataSetFormattersAndIds.{serializableDataSetSettingFormat, serializableDataSpaceMetaInfoFormat, serializableBSONObjectIDFormat, DataSetSettingIdentity}
  import org.ada.server.models.User.{serializableUserFormat, UserIdentity}
  import org.ada.server.models.HtmlSnippet.{serializableHtmlSnippetFormat, HtmlSnippetIdentity}

  def configure = {

    implicit val formatId = serializableBSONObjectIDFormat

    bind[DataSetSettingRepo].toProvider(
      new CacheAsyncCrudRepoProvider[DataSetSetting, BSONObjectID]("dataset_settings")
    ).asEagerSingleton

    bind[UserRepo].toProvider(
      new CacheAsyncCrudRepoProvider[User, BSONObjectID]("users")
    ).asEagerSingleton

    bind[DataSpaceMetaInfoRepo].toProvider(
      new CacheAsyncCrudRepoProvider[DataSpaceMetaInfo, BSONObjectID]("dataspace_meta_infos")
    ).asEagerSingleton

    bind[HtmlSnippetRepo].toProvider(
      new CacheAsyncCrudRepoProvider[HtmlSnippet, BSONObjectID]("html_snippets")
    ).asEagerSingleton

    bind[ElasticClient].toProvider(new PlayElasticClientProvider).asEagerSingleton

    // bind the repos defined above
    RepoDef.values.foreach(bindRepo(_))

    bind[DataSetAccessorFactory].to(classOf[DataSetAccessorFactoryImpl]).asEagerSingleton

    // install JSON repo factories and its cached version
    install(new FactoryModuleBuilder()
      .implement(new TypeLiteral[JsonCrudRepo]{}, classOf[MongoJsonCrudRepo])
      .build(Key.get(classOf[MongoJsonCrudRepoFactory], Names.named("MongoJsonCrudRepoFactory"))))

    install(new FactoryModuleBuilder()
      .implement(new TypeLiteral[JsonCrudRepo]{}, classOf[ElasticJsonCrudRepo])
      .build(Key.get(classOf[JsonCrudRepoFactory], Names.named("ElasticJsonCrudRepoFactory"))))

    bind[MongoJsonCrudRepoFactory]
      .annotatedWith(Names.named("CachedJsonCrudRepoFactory"))
      .to(classOf[JsonBinaryCacheAsyncCrudRepoFactory])

    install(new FactoryModuleBuilder()
      .implement(new TypeLiteral[ClassificationResultRepo]{}, classOf[ClassificationResultMongoAsyncCrudRepo])
      .build(classOf[ClassificationResultRepoFactory]))

    install(new FactoryModuleBuilder()
      .implement(new TypeLiteral[RegressionResultRepo]{}, classOf[RegressionResultMongoAsyncCrudRepo])
      .build(classOf[RegressionResultRepoFactory]))

    // install data set meta info repo factory
//    install(new FactoryModuleBuilder()
//      .implement(new TypeLiteral[DataSetMetaInfoRepo]{}, classOf[DataSetMetaInfoSubordinateMongoAsyncCrudRepo])
//      .build(classOf[DataSetMetaInfoRepoFactory]))

    // install dictionary field repo factory

//    install(new FactoryModuleBuilder()
//      .implement(new TypeLiteral[FieldRepo]{}, classOf[DictionaryFieldMongoAsyncCrudRepo])
//      .build(classOf[FieldRepoFactory]))

    // install dictionary category repo factory
//    install(new FactoryModuleBuilder()
//      .implement(new TypeLiteral[CategoryRepo]{}, classOf[DictionaryCategoryMongoAsyncCrudRepo])
//      .build(classOf[CategoryRepoFactory]))
  }

  private def bindRepo[T](repo : Repo[T]) = {
    implicit val manifest = repo.man
    if (repo.named)
      bind[T]
        .annotatedWith(Names.named(repo.toString))
        .toInstance(repo.repo)
    else
      bind[T].toInstance(repo.repo)
  }
}