package org.ada.server.models.dataimport

import java.util.Date

import org.ada.server.models.{DataSetSetting, DataView}
import reactivemongo.bson.BSONObjectID

case class TranSmartDataSetImport(
  _id: Option[BSONObjectID] = None,
  dataSpaceName: String,
  dataSetId: String,
  dataSetName: String,
  dataPath: Option[String] = None,
  mappingPath: Option[String] = None,
  charsetName: Option[String] = None,
  matchQuotes: Boolean,
  inferFieldTypes: Boolean,
  inferenceMaxEnumValuesCount: Option[Int] = None,
  inferenceMinAvgValuesPerEnum: Option[Double] = None,
  saveBatchSize: Option[Int] = None,
  scheduled: Boolean = false,
  scheduledTime: Option[ScheduledTime] = None,
  setting: Option[DataSetSetting] = None,
  dataView: Option[DataView] = None,
  timeCreated: Date = new Date(),
  var timeLastExecuted: Option[Date] = None
) extends DataSetImport
