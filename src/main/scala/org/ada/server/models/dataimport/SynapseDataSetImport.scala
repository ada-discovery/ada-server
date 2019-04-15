package org.ada.server.models.dataimport

import java.util.Date

import org.ada.server.models.{DataSetSetting, DataView}
import reactivemongo.bson.BSONObjectID

case class SynapseDataSetImport(
  _id: Option[BSONObjectID] = None,
  dataSpaceName: String,
  dataSetId: String,
  dataSetName: String,
  tableId: String,
  downloadColumnFiles: Boolean,
  batchSize: Option[Int] = None,
  bulkDownloadGroupNumber: Option[Int] = None,
  scheduled: Boolean = false,
  scheduledTime: Option[ScheduledTime] = None,
  setting: Option[DataSetSetting] = None,
  dataView: Option[DataView] = None,
  timeCreated: Date = new Date(),
  var timeLastExecuted: Option[Date] = None
) extends DataSetImport
