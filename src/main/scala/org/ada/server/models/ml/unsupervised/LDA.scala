package org.ada.server.models.ml.unsupervised

import java.util.Date

import reactivemongo.bson.BSONObjectID

case class LDA(
  _id: Option[BSONObjectID],
  k: Int,
  maxIteration: Option[Int] = None,
  seed: Option[Long] = None,
  checkpointInterval: Option[Int] = None,
  docConcentration: Option[Seq[Double]] = None,
  topicConcentration: Option[Double] = None,
  optimizer: Option[LDAOptimizer.Value] = None,
  learningOffset: Option[Double] = None,
  learningDecay: Option[Double] = None,
  subsamplingRate: Option[Double] = None,
  optimizeDocConcentration: Option[Boolean] = None,
  keepLastCheckpoint: Option[Boolean] = None,
  name: Option[String] = None,
  createdById: Option[BSONObjectID] = None,
  timeCreated: Date = new Date()
) extends UnsupervisedLearning

object LDAOptimizer extends Enumeration {
  val online = Value("online")
  val em = Value("em")
}