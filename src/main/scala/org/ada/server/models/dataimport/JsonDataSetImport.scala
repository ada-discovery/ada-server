package org.ada.server.models.dataimport

import java.util.Date

import org.ada.server.models.{DataSetSetting, DataView, ScheduledTime}
import reactivemongo.bson.BSONObjectID

case class JsonDataSetImport(
  _id: Option[BSONObjectID] = None,
  dataSpaceName: String,
  dataSetId: String,
  dataSetName: String,
  path: Option[String] = None,
  charsetName: Option[String] = None,
  inferFieldTypes: Boolean,
  inferenceMaxEnumValuesCount: Option[Int] = None,
  inferenceMinAvgValuesPerEnum: Option[Double] = None,
  booleanIncludeNumbers: Boolean = false,
  saveBatchSize: Option[Int] = None,
  scheduled: Boolean = false,
  scheduledTime: Option[ScheduledTime] = None,
  setting: Option[DataSetSetting] = None,
  dataView: Option[DataView] = None,
  timeCreated: Date = new Date(),
  timeLastExecuted: Option[Date] = None
) extends DataSetImport
