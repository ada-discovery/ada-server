package org.ada.server.dataaccess.dataset

import javax.inject.{Inject, Named, Singleton}

import org.ada.server.dataaccess._
import org.ada.server.models.DataSetFormattersAndIds.DataSetMetaInfoIdentity
import org.ada.server.models._
import org.ada.server.dataaccess.RepoTypes._
import org.ada.server.dataaccess.dataset.RefreshableCache
import org.incal.core.dataaccess.Criterion.Infix
import play.api.Logger
import play.api.libs.json.JsObject
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import reactivemongo.bson.BSONObjectID

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.Await._

trait DataSetAccessorFactory {

  def register(
    dataSpaceName: String,
    dataSetId: String,
    dataSetName: String,
    setting: Option[DataSetSetting],
    dataView: Option[DataView]
  ): Future[DataSetAccessor]

  def register(
    metaInfo: DataSetMetaInfo,
    setting: Option[DataSetSetting],
    dataView: Option[DataView]
  ): Future[DataSetAccessor]

  def apply(dataSetId: String): Option[DataSetAccessor]

  @Deprecated
  def dataSetRepoCreate(
    dataSetId: String)(
    fieldNamesAndTypes: Seq[(String, FieldTypeSpec)],
    dataSetSetting: Option[DataSetSetting] = None
  ): Future[JsonCrudRepo]
}

@Singleton
protected[dataaccess] class DataSetAccessorFactoryImpl @Inject()(
    @Named("MongoJsonCrudRepoFactory") mongoDataSetRepoFactory: MongoJsonCrudRepoFactory,
    @Named("ElasticJsonCrudRepoFactory") elasticDataSetRepoFactory: ElasticJsonCrudRepoFactory,
    @Named("CachedJsonCrudRepoFactory") cachedDataSetRepoFactory: MongoJsonCrudRepoFactory,
    fieldRepoFactory: FieldRepoFactory,
    categoryRepoFactory: CategoryRepoFactory,
    filterRepoFactory: FilterRepoFactory,
    dataViewRepoFactory: DataViewRepoFactory,
    classificationResultRepoFactory: ClassificationResultRepoFactory,
    regressionResultRepoFactory: RegressionResultRepoFactory,
    dataSetMetaInfoRepoFactory: DataSetMetaInfoRepoFactory,
    dataSpaceMetaInfoRepo: DataSpaceMetaInfoRepo,
    dataSetSettingRepo: DataSetSettingRepo
  ) extends RefreshableCache[String, DataSetAccessor](false) with DataSetAccessorFactory {

  override protected def createInstance(
    dataSetId: String
  ): Future[Option[DataSetAccessor]] =
    for {
      dataSpaceId <-
      // TODO: dataSpaceMetaInfoRepo is cached and so querying nested objects "dataSetMetaInfos.id" does not work properly
      //        dataSpaceMetaInfoRepo.find(
      //          Seq("dataSetMetaInfos.id" #== dataSetId)
      //        ).map(_.headOption.map(_._id.get))
      dataSpaceMetaInfoRepo.find().map ( dataSpaceMetaInfos =>
        dataSpaceMetaInfos.find(_.dataSetMetaInfos.map(_.id).contains(dataSetId)).map(_._id.get)
      )
    } yield
      dataSpaceId.map( spaceId =>
        createInstanceAux(dataSetId, spaceId)
      )

  override protected def createInstances(
    dataSetIds: Traversable[String]
  ): Future[Traversable[(String, DataSetAccessor)]] =
    for {
      dataSetSpaceIds <-
        dataSpaceMetaInfoRepo.find().map( dataSpaceMetaInfos =>
          dataSetIds.map( dataSetId =>
            dataSpaceMetaInfos.find(_.dataSetMetaInfos.map(_.id).contains(dataSetId)).map( dataSpace =>
              (dataSetId, dataSpace._id.get)
            )
          ).flatten
        )
    } yield
      dataSetSpaceIds.map { case (dataSetId, spaceId) =>
        val accessor = createInstanceAux(dataSetId, spaceId)
        (dataSetId, accessor)
      }

  private def createInstanceAux(
    dataSetId: String,
    dataSpaceId: BSONObjectID
  ): DataSetAccessor = {
    val fieldRepo = fieldRepoFactory(dataSetId)
    val categoryRepo = categoryRepoFactory(dataSetId)
    val filterRepo = filterRepoFactory(dataSetId)
    val dataViewRepo = dataViewRepoFactory(dataSetId)
    val classificationResultRepo = classificationResultRepoFactory(dataSetId)
    val regressionResultRepo = regressionResultRepoFactory(dataSetId)

    new DataSetAccessorImpl(
      dataSetId,
      fieldRepo,
      categoryRepo,
      filterRepo,
      dataViewRepo,
      classificationResultRepo,
      regressionResultRepo,
      dataSetRepoCreate(dataSetId),
      dataSetMetaInfoRepoFactory.apply,
      dataSetMetaInfoRepoFactory(dataSpaceId),
      dataSetSettingRepo
    )
  }

  def dataSetRepoCreate(
    dataSetId: String)(
    fieldNamesAndTypes: Seq[(String, FieldTypeSpec)],
    dataSetSetting: Option[DataSetSetting] = None
  ): Future[JsonCrudRepo] = {
    val collectionName = dataCollectionName(dataSetId)

    for {
      dataSetSetting <-
        dataSetSetting match {
          case Some(dataSetSetting) => Future(Some(dataSetSetting))
          case None => dataSetSettingRepo.find(Seq("dataSetId" #== dataSetId)).map(_.headOption)
        }
    } yield {
      val cacheDataSet =
        dataSetSetting.map(_.cacheDataSet).getOrElse(false)

      val storageType =
        dataSetSetting.map(_.storageType).getOrElse(StorageType.Mongo)

      val mongoAutoCreateIndex =
        dataSetSetting.map(_.mongoAutoCreateIndexForProjection).getOrElse(false)

      if (cacheDataSet) {
        println(s"Creating cached data set repo for '$dataSetId'.")
        cachedDataSetRepoFactory(collectionName, fieldNamesAndTypes, mongoAutoCreateIndex)
      } else
        storageType match {
          case StorageType.Mongo => {
            println(s"Creating Mongo based data set repo for '$dataSetId'.")
            mongoDataSetRepoFactory(collectionName, fieldNamesAndTypes, mongoAutoCreateIndex)
          }
          case StorageType.ElasticSearch => {
            println(s"Creating Elastic Search based data set repo for '$dataSetId'.")
            elasticDataSetRepoFactory(collectionName, collectionName, fieldNamesAndTypes, None, false)
          }
        }
      }
    }

  override def register(
    metaInfo: DataSetMetaInfo,
    setting: Option[DataSetSetting],
    dataView: Option[DataView]
  ) = {
    val dataSetMetaInfoRepo = dataSetMetaInfoRepoFactory(metaInfo.dataSpaceId)
    val existingMetaInfosFuture = dataSetMetaInfoRepo.find(Seq("id" #== metaInfo.id)).map(_.headOption)
    val existingSettingFuture = dataSetSettingRepo.find(Seq("dataSetId" #== metaInfo.id)).map(_.headOption)

    for {
      existingMetaInfos <- existingMetaInfosFuture
      existingSetting <- existingSettingFuture
      dsa <- registerAux(dataSetMetaInfoRepo, metaInfo, setting, existingMetaInfos, existingSetting, dataView)
    } yield
      dsa
  }

  private def registerAux(
    dataSetMetaInfoRepo: DataSetMetaInfoRepo,
    metaInfo: DataSetMetaInfo,
    setting: Option[DataSetSetting],
    existingMetaInfo: Option[DataSetMetaInfo],
    existingSetting: Option[DataSetSetting],
    dataView: Option[DataView]
  ) = {
    // register setting
    val settingFuture = setting.map( setting =>
      existingSetting.map( existingSetting =>
        // if already exists then update a storage type
        dataSetSettingRepo.update(existingSetting.copy(storageType = setting.storageType))
      ).getOrElse(
        // otherwise save
        dataSetSettingRepo.save(setting)
      )
    ).getOrElse(
      // no setting provided
      existingSetting.map( _ =>
        // if already exists, do nothing
        Future(())
      ).getOrElse(
        // otherwise save a dummy one
        dataSetSettingRepo.save(new DataSetSetting(metaInfo.id))
      )
    )

    // register meta info
    val metaInfoFuture = updateMetaInfo(dataSetMetaInfoRepo, metaInfo, existingMetaInfo)

    for {
      // execute the setting registration
      _ <- settingFuture

      // execute the meta info registration
      _ <- metaInfoFuture

      // create a data set accessor (and data view repo)
      dsa <- cache.get(metaInfo.id).map(
        Future(_)
      ).getOrElse(
        createInstance(metaInfo.id).map { case Some(dsa) =>
          cache.update(metaInfo.id, dsa)
          dsa
        }
      )

      dataViewRepo = dsa.dataViewRepo

      // check if the data view exist
      dataViewExist <- dataViewRepo.count().map(_ > 0)

      // register (save) data view if none view already exists
      _ <- if (!dataViewExist && dataView.isDefined)
        dsa.dataViewRepo.save(dataView.get)
      else
        Future(())
    } yield
      dsa
  }

  private def updateMetaInfo(
    dataSetMetaInfoRepo: DataSetMetaInfoRepo,
    metaInfo: DataSetMetaInfo,
    existingMetaInfo: Option[DataSetMetaInfo]
  ): Future[BSONObjectID] =
    existingMetaInfo.map( existingMetaInfo =>
      for {
        // if already exists update the name
        metaInfoId <- dataSetMetaInfoRepo.update(existingMetaInfo.copy(name = metaInfo.name))

        // receive an associated data space meta info
        dataSpaceMetaInfo <- dataSpaceMetaInfoRepo.get(existingMetaInfo.dataSpaceId)

        _ <- dataSpaceMetaInfo match {
          // find a data set info and update its name, replace it in the data space, and update
          case Some(dataSpaceMetaInfo) =>
            val metaInfos = dataSpaceMetaInfo.dataSetMetaInfos
            val metaInfoInSpace = metaInfos.find(_._id == existingMetaInfo._id).get
            val unchangedMetaInfos = metaInfos.filterNot(_._id == existingMetaInfo._id)

            dataSpaceMetaInfoRepo.update(
              dataSpaceMetaInfo.copy(dataSetMetaInfos = (unchangedMetaInfos ++ Seq(metaInfoInSpace.copy(name = metaInfo.name))))
            )
          case None => Future(())
        }
      } yield
        metaInfoId

    ).getOrElse(
      for {
        // if it doesn't exists save a new one
        metaInfoId <- dataSetMetaInfoRepo.save(metaInfo)

        // receive an associated data space meta info
        dataSpaceMetaInfo <- dataSpaceMetaInfoRepo.get(metaInfo.dataSpaceId)

        _ <- dataSpaceMetaInfo match {
          // add a new data set info to the data space and update
          case Some(dataSpaceMetaInfo) =>
            val metaInfoWithId = DataSetMetaInfoIdentity.set(metaInfo, metaInfoId)

            dataSpaceMetaInfoRepo.update(
              dataSpaceMetaInfo.copy(dataSetMetaInfos = (dataSpaceMetaInfo.dataSetMetaInfos ++ Seq(metaInfoWithId)))
            )
          case None => Future(())
        }
      } yield
        metaInfoId
    )

  override def register(
    dataSpaceName: String,
    dataSetId: String,
    dataSetName: String,
    setting: Option[DataSetSetting],
    dataView: Option[DataView]
  ) = for {
      // search for data spaces with a given name
      spaces <- dataSpaceMetaInfoRepo.find(Seq("name" #== dataSpaceName))
      // get an id from an existing data space or create a new one
      spaceId <- spaces.headOption.map(space => Future(space._id.get)).getOrElse(
        dataSpaceMetaInfoRepo.save(DataSpaceMetaInfo(None, dataSpaceName, 0))
      )
      // register data set meta info and setting, and obtain an accessor
      accessor <- {
        val metaInfo = DataSetMetaInfo(id = dataSetId, name = dataSetName, dataSpaceId = spaceId)
        register(metaInfo, setting, dataView)
      }
    } yield
      accessor

  override protected def getAllIds =
    dataSpaceMetaInfoRepo.find().map(
      _.map(_.dataSetMetaInfos.map(_.id)).flatten
    )

  private def dataCollectionName(dataSetId: String) = "data-" + dataSetId
}