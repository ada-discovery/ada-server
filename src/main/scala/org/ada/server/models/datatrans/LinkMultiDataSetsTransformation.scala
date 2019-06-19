package org.ada.server.models.datatrans

import java.util.Date

import org.ada.server.dataaccess.StreamSpec
import org.ada.server.models.ScheduledTime
import reactivemongo.bson.BSONObjectID

case class LinkMultiDataSetsTransformation(
  _id: Option[BSONObjectID] = None,

  linkedDataSetSpecs: Seq[LinkedDataSetSpec],
  addDataSetIdToRightFieldNames: Boolean,

  resultDataSetSpec: ResultDataSetSpec,
  streamSpec: StreamSpec,
  scheduled: Boolean = false,
  scheduledTime: Option[ScheduledTime] = None,
  timeCreated: Date = new Date(),
  timeLastExecuted: Option[Date] = None
) extends DataSetTransformation {
  override val sourceDataSetIds = linkedDataSetSpecs.map(_.dataSetId)
}

case class LinkedDataSetSpec(
  dataSetId: String,
  linkFieldNames: Seq[String],
  explicitFieldNamesToKeep: Traversable[String] = Nil
)