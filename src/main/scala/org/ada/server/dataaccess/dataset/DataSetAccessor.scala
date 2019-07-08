package org.ada.server.dataaccess.dataset

import org.ada.server.dataaccess.ElasticJsonCrudRepoFactory
import org.ada.server.models.{DataSetMetaInfo, DataSetSetting, FieldTypeSpec}
import org.incal.core.dataaccess.Criterion.Infix

import scala.concurrent.duration._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import reactivemongo.bson.BSONObjectID

import scala.concurrent.Future
import scala.concurrent.Await.result
import org.ada.server.dataaccess.RepoTypes._
import org.ada.server.dataaccess.SubTypeBasedAsyncCrudRepo

trait DataSetAccessor {
  def dataSetId: String
  def fieldRepo: FieldRepo
  def categoryRepo: CategoryRepo
  def filterRepo: FilterRepo
  def dataViewRepo: DataViewRepo

  // ML

  def classificationResultRepo: ClassificationResultRepo
  def standardClassificationRepo: StandardClassificationResultRepo
  def temporalClassificationRepo: TemporalClassificationResultRepo

  def regressionResultRepo: RegressionResultRepo
  def standardRegressionResultRepo: StandardRegressionResultRepo
  def temporalRegressionResultRepo: TemporalRegressionResultRepo

  // following attributes are dynamically created, i.e., each time the respective function is called

  def dataSetRepo: JsonCrudRepo
  def metaInfo: Future[DataSetMetaInfo]
  def dataSetName = metaInfo.map(_.name)
  def setting: Future[DataSetSetting]

  // functions to refresh a few attributes

  def updateDataSetRepo: Future[Unit]
  def updateDataSetRepo(setting: DataSetSetting): Future[Unit]
  def updateSetting(setting: DataSetSetting): Future[BSONObjectID]
  def updateMetaInfo(metaInfo: DataSetMetaInfo): Future[BSONObjectID]
}

protected class DataSetAccessorImpl(
    val dataSetId: String,
    val fieldRepo: FieldRepo,
    val categoryRepo: CategoryRepo,
    val filterRepo: FilterRepo,
    val dataViewRepo: DataViewRepo,
    val classificationResultRepo: ClassificationResultRepo,
    val regressionResultRepo: RegressionResultRepo,
    dataSetRepoCreate: (Seq[(String, FieldTypeSpec)], Option[DataSetSetting]) => Future[JsonCrudRepo],
    dataSetMetaInfoRepoCreate: BSONObjectID => DataSetMetaInfoRepo,
    initDataSetMetaInfoRepo: DataSetMetaInfoRepo,
    dataSetSettingRepo: DataSetSettingRepo
  ) extends DataSetAccessor {

  private var _dataSetRepo: Option[JsonCrudRepo] = None
  private var dataSetMetaInfoRepo = initDataSetMetaInfoRepo

  override def dataSetRepo = {
    if (_dataSetRepo.isEmpty) {
      _dataSetRepo = Some(result(createDataSetRepo(None), 10 seconds))
    }
    _dataSetRepo.get
  }

  private def createDataSetRepo(dataSetSetting: Option[DataSetSetting]) =
    for {
      fields <- fieldRepo.find()
      dataSetRepo <- {
        val fieldNamesAndTypes = fields.map(field => (field.name, field.fieldTypeSpec)).toSeq
        dataSetRepoCreate(fieldNamesAndTypes, dataSetSetting)
      }
    } yield
      dataSetRepo

  override def updateDataSetRepo(setting: DataSetSetting) =
    for {
      newDataSetRepo <- createDataSetRepo(Some(setting))
    } yield
      _dataSetRepo = Some(newDataSetRepo)

  override def updateDataSetRepo =
    for {
      newDataSetRepo <- createDataSetRepo(None)
    } yield
      _dataSetRepo = Some(newDataSetRepo)

  override def setting =
    for {
      settings <- dataSetSettingRepo.find(Seq("dataSetId" #== dataSetId))
    } yield
      settings.headOption.getOrElse(
        throw new IllegalStateException("Setting not available for data set '" + dataSetId + "'.")
      )

  override def updateSetting(setting: DataSetSetting) =
    dataSetSettingRepo.update(setting)

  // meta info

  override def metaInfo =
    for {
      metaInfos <- dataSetMetaInfoRepo.find(Seq("id" #== dataSetId))
    } yield
      metaInfos.headOption.getOrElse(
        throw new IllegalStateException("Meta info not available for data set '" + dataSetId + "'.")
      )

  override def updateMetaInfo(metaInfo: DataSetMetaInfo) = {
    dataSetMetaInfoRepo = dataSetMetaInfoRepoCreate(metaInfo.dataSpaceId)
    dataSetMetaInfoRepo.update(metaInfo)
  }

  // ML extra

  override val standardClassificationRepo: StandardClassificationResultRepo =
    SubTypeBasedAsyncCrudRepo(classificationResultRepo)

  override val temporalClassificationRepo: TemporalClassificationResultRepo =
    SubTypeBasedAsyncCrudRepo(classificationResultRepo)

  override val standardRegressionResultRepo: StandardRegressionResultRepo =
    SubTypeBasedAsyncCrudRepo(regressionResultRepo)

  override val temporalRegressionResultRepo: TemporalRegressionResultRepo =
    SubTypeBasedAsyncCrudRepo(regressionResultRepo)
}