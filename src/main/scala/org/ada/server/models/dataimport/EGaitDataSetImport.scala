package org.ada.server.models.dataimport

import java.util.Date

import org.ada.server.models.{DataSetSetting, DataView}
import reactivemongo.bson.BSONObjectID

case class EGaitDataSetImport(
  _id: Option[BSONObjectID] = None,
  dataSpaceName: String,
  dataSetId: String,
  dataSetName: String,
  importRawData: Boolean = false,
  scheduled: Boolean = false,
  scheduledTime: Option[ScheduledTime] = None,
  setting: Option[DataSetSetting] = None,
  dataView: Option[DataView] = None,
  timeCreated: Date = new Date(),
  var timeLastExecuted: Option[Date] = None
) extends DataSetImport