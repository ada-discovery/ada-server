package org.ada.server.models.ml.unsupervised

import java.util.Date

import reactivemongo.bson.BSONObjectID

case class GaussianMixture(
  _id: Option[BSONObjectID],
  k: Int,
  maxIteration: Option[Int] = None,
  tolerance: Option[Double] = None,
  seed: Option[Long] = None,
  name: Option[String] = None,
  createdById: Option[BSONObjectID] = None,
  timeCreated: Date = new Date()
) extends UnsupervisedLearning
