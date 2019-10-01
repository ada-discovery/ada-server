package org.ada.server.models.ml.classification

import org.ada.server.dataaccess.BSONObjectIdentity
import org.ada.server.json._
import play.api.libs.json.{Format, Json}
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats._

import org.incal.spark_ml.models.classification._
import org.incal.spark_ml.models.TreeCore

object Classifier {

  implicit val logisticModelFamilyEnumTypeFormat = EnumFormat(LogisticModelFamily)
  implicit val mlpSolverEnumTypeFormat = EnumFormat(MLPSolver)
  implicit val featureSubsetStrategyEnumTypeFormat = EnumFormat(RandomForestFeatureSubsetStrategy)
  implicit val decisionTreeImpurityEnumTypeFormat = EnumFormat(DecisionTreeImpurity)
  implicit val gbtClassificationLossTypeEnumTypeFormat = EnumFormat(GBTClassificationLossType)
  implicit val bayesModelTypeEnumTypeFormat = EnumFormat(BayesModelType)

  def eitherFormat[T: Format] = {
    implicit val optionFormat = new OptionFormat[T]
    EitherFormat[Option[T], Seq[T]]
  }

  implicit val doubleEitherFormat = eitherFormat[Double]
  implicit val intEitherFormat = eitherFormat[Int]

  private implicit val treeCoreFormat = Json.format[TreeCore]

  implicit val classifierFormat: Format[Classifier] = new SubTypeFormat[Classifier](
    Seq(
      RuntimeClassFormat(Json.format[LogisticRegression]),
      RuntimeClassFormat(Json.format[MultiLayerPerceptron]),
      RuntimeClassFormat(Json.format[DecisionTree]),
      RuntimeClassFormat(Json.format[RandomForest]),
      RuntimeClassFormat(Json.format[GradientBoostTree]),
      RuntimeClassFormat(Json.format[NaiveBayes]),
      RuntimeClassFormat(Json.format[LinearSupportVectorMachine])
    )
  )

  implicit object ClassifierIdentity extends BSONObjectIdentity[Classifier] {
    def of(entity: Classifier): Option[BSONObjectID] = entity._id

    protected def set(entity: Classifier, id: Option[BSONObjectID]) =
      entity match {
        case x: LogisticRegression => x.copy(_id = id)
        case x: MultiLayerPerceptron => x.copy(_id = id)
        case x: DecisionTree => x.copy(_id = id)
        case x: RandomForest => x.copy(_id = id)
        case x: GradientBoostTree => x.copy(_id = id)
        case x: NaiveBayes => x.copy(_id = id)
        case x: LinearSupportVectorMachine => x.copy(_id = id)
      }
  }
}