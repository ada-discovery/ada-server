package org.ada.server.models.ml.clustering

import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats._
import org.ada.server.dataaccess.BSONObjectIdentity
import org.ada.server.json.{EnumFormat, RuntimeClassFormat, SubTypeFormat}
import org.incal.spark_ml.models.clustering._
import play.api.libs.json.{Format, Json}

object Clustering {
  implicit val KMeansInitModeEnumTypeFormat = EnumFormat(KMeansInitMode)
  implicit val LDAOptimizerEnumTypeFormat = EnumFormat(LDAOptimizer)

  implicit val clusteringFormat: Format[Clustering] = new SubTypeFormat[Clustering](
    Seq(
      RuntimeClassFormat(Json.format[KMeans]),
      RuntimeClassFormat(Json.format[LDA]),
      RuntimeClassFormat(Json.format[BisectingKMeans]),
      RuntimeClassFormat(Json.format[GaussianMixture])
    )
  )

  implicit object ClusteringIdentity extends BSONObjectIdentity[Clustering] {
    def of(entity: Clustering): Option[BSONObjectID] = entity._id

    protected def set(entity: Clustering, id: Option[BSONObjectID]) =
      entity match {
        case x: KMeans => x.copy(_id = id)
        case x: LDA => x.copy(_id = id)
        case x: BisectingKMeans => x.copy(_id = id)
        case x: GaussianMixture => x.copy(_id = id)
      }
  }
}