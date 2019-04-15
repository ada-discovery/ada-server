package org.ada.server.models.egait

import play.api.libs.json.Json
import reactivemongo.bson.BSONObjectID

case class EGaitKineticData(
  sessionId: String,
  personId: String,
  instructor: String,
  startTime: java.util.Date,
  testName: String,
  testDuration: Int,
  rightSensorFileName: String,
  leftSensorFileName: String,
  rightSensorStartIndex: Int,
  rightSensorStopIndex: Int,
  leftSensorStartIndex: Int,
  leftSensorStopIndex: Int,
  rightAccelerometerPoints: Seq[SpatialPoint],
  rightGyroscopePoints: Seq[SpatialPoint],
  leftAccelerometerPoints: Seq[SpatialPoint],
  leftGyroscopePoints: Seq[SpatialPoint]
)

case class SpatialPoint(x: Int, y: Int, z: Int)

object EGaitKineticData {
  implicit val spatialPointFormat = Json.format[SpatialPoint]
  implicit val eGaitSessionFormat = Json.format[EGaitKineticData]
}