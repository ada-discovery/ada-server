package org.ada.server.models.dataimport

import java.util.Date

import org.ada.server.models.{DataSetSetting, DataView, ScheduledTime}
import reactivemongo.bson.BSONObjectID

case class RedCapDataSetImport(
  _id: Option[BSONObjectID] = None,
  dataSpaceName: String,
  dataSetId: String,
  dataSetName: String,
  url: String,
  token: String,
  importDictionaryFlag: Boolean,
  eventNames: Seq[String] = Nil,
  categoriesToInheritFromFirstVisit: Seq[String] = Nil,
  saveBatchSize: Option[Int] = None,
  scheduled: Boolean = false,
  scheduledTime: Option[ScheduledTime] = None,
  setting: Option[DataSetSetting] = None,
  dataView: Option[DataView] = None,
  timeCreated: Date = new Date(),
  timeLastExecuted: Option[Date] = None
) extends DataSetImport
