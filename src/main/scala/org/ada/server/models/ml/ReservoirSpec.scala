package org.ada.server.models.ml

import com.bnd.math.domain.rand.RandomDistribution
import com.bnd.network.domain.ActivationFunctionType
import org.ada.server.json.{EitherFormat, JavaEnumFormat, OptionFormat}
import org.incal.spark_ml.models.{ReservoirSpec => ReservoirSpecModel}
import org.incal.spark_ml.models.ValueOrSeq.ValueOrSeq
import play.api.libs.json.{Format, __}
import play.api.libs.functional.syntax._
import java.{lang => jl}

object ReservoirSpec {

  def eitherFormat[T: Format] = {
    implicit val optionFormat = new OptionFormat[T]
    EitherFormat[Option[T], Seq[T]]
  }

  implicit val doubleEitherFormat = eitherFormat[Double]
  implicit val intEitherFormat = eitherFormat[Int]
  implicit val activationFunctionTypeFormat = JavaEnumFormat[ActivationFunctionType]

  private val normalDistribution = RandomDistribution.createNormalDistribution(classOf[jl.Double], 0d, 1d)

  // TODO: handle random distributions
  implicit val reservoirSpecFormat: Format[ReservoirSpecModel] = (
    (__ \ "inputNodeNum").format[Int] and
    (__ \ "bias").format[Double] and
    (__ \ "nonBiasInitial").format[Double] and
    (__ \ "reservoirNodeNum").format[ValueOrSeq[Int]] and
    (__ \ "reservoirInDegree").format[ValueOrSeq[Int]] and
    (__ \ "reservoirEdgesNum").format[ValueOrSeq[Int]] and
//    (__ \ "reservoirInDegreeDistribution").format[Option[RandomDistribution[Integer]]] and
    (__ \ "reservoirCircularInEdges").formatNullable[Seq[Int]] and
    (__ \ "reservoirPreferentialAttachment").format[Boolean] and
    (__ \ "reservoirBias").format[Boolean] and
    (__ \ "reservoirAllowSelfEdges").format[Boolean] and
    (__ \ "reservoirAllowMultiEdges").format[Boolean] and
    (__ \ "inputReservoirConnectivity").format[ValueOrSeq[Double]] and
//    (__ \ "weightDistribution").format[RandomDistribution[jl.Double]] and
    (__ \ "reservoirSpectralRadius").format[ValueOrSeq[Double]] and
    (__ \ "reservoirFunctionType").format[ActivationFunctionType] and
    (__ \ "reservoirFunctionParams").format[Seq[Double]] and
//    (__ \ "perNodeReservoirFunctionWithParams").format[Option[Stream[(ActivationFunctionType, Seq[jl.Double])]]] and
    (__ \ "washoutPeriod").format[ValueOrSeq[Int]]
  )(
    ReservoirSpecModel(_, _, _, _, _, _, None, _, _, _, _, _, _, normalDistribution, _, _, _, None, _),
    x => (
      x.inputNodeNum, x.bias, x.nonBiasInitial, x.reservoirNodeNum,
      x.reservoirInDegree, x.reservoirEdgesNum, x.reservoirCircularInEdges,
      x.reservoirPreferentialAttachment, x.reservoirBias, x.reservoirAllowSelfEdges,
      x.reservoirAllowMultiEdges, x.inputReservoirConnectivity, x.reservoirSpectralRadius,
      x.reservoirFunctionType, x.reservoirFunctionParams, x.washoutPeriod
    )
  )
}