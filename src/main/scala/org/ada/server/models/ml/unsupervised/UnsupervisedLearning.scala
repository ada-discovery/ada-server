package org.ada.server.models.ml.unsupervised

import java.util.Date

import reactivemongo.play.json.BSONFormats._
import org.ada.server.dataaccess.BSONObjectIdentity
import org.ada.server.json.{EnumFormat, ManifestedFormat, SubTypeFormat}
import play.api.libs.json.{Format, Json}
import reactivemongo.bson.BSONObjectID

abstract class UnsupervisedLearning {
  val _id: Option[BSONObjectID]
  val name: Option[String]
  val createdById: Option[BSONObjectID]
  val timeCreated: Date
}

object UnsupervisedLearning {
  implicit val KMeansInitModeEnumTypeFormat = EnumFormat(KMeansInitMode)
  implicit val LDAOptimizerEnumTypeFormat = EnumFormat(LDAOptimizer)

  implicit val unsupervisedLearningFormat: Format[UnsupervisedLearning] = new SubTypeFormat[UnsupervisedLearning](
    Seq(
      ManifestedFormat(Json.format[KMeans]),
      ManifestedFormat(Json.format[LDA]),
      ManifestedFormat(Json.format[BisectingKMeans]),
      ManifestedFormat(Json.format[GaussianMixture])
    )
  )

  implicit object UnsupervisedLearningIdentity extends BSONObjectIdentity[UnsupervisedLearning] {
    def of(entity: UnsupervisedLearning): Option[BSONObjectID] = entity._id

    protected def set(entity: UnsupervisedLearning, id: Option[BSONObjectID]) =
      entity match {
        case x: KMeans => x.copy(_id = id)
        case x: LDA => x.copy(_id = id)
        case x: BisectingKMeans => x.copy(_id = id)
        case x: GaussianMixture => x.copy(_id = id)
      }
  }
}
