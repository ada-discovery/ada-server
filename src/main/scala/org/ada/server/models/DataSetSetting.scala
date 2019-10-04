package org.ada.server.models

import reactivemongo.bson.BSONObjectID

case class DataSetSetting(
  _id: Option[BSONObjectID],
  dataSetId: String,
  keyFieldName: String,
  exportOrderByFieldName: Option[String] = None,
  defaultScatterXFieldName: Option[String] = None,
  defaultScatterYFieldName: Option[String] = None,
  defaultDistributionFieldName: Option[String] = None,
  defaultCumulativeCountFieldName: Option[String] = None,
  filterShowFieldStyle: Option[FilterShowFieldStyle.Value] = None,
  filterShowNonNullCount: Boolean = false,
  displayItemName: Option[String] = None,
  storageType: StorageType.Value,
  mongoAutoCreateIndexForProjection: Boolean = false,
  cacheDataSet: Boolean = false,
  ownerId: Option[BSONObjectID] = None,
  showSideCategoricalTree: Boolean = true,
  extraNavigationItems: Seq[NavigationItem] = Nil,
  customControllerClassName: Option[String] = None,
  description: Option[String] = None
) {
  def this(
    dataSetId: String,
    storageType: StorageType.Value
  ) =
    this(_id = None, dataSetId = dataSetId, keyFieldName = "_id", storageType = storageType)

  def this(dataSetId: String) =
    this(dataSetId, StorageType.Mongo)
}

object StorageType extends Enumeration {
  val Mongo = Value("Mongo")
  val ElasticSearch = Value("Elastic Search")
}