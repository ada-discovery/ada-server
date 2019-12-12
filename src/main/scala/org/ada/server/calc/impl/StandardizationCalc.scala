package org.ada.server.calc.impl

import akka.stream.scaladsl.{Flow, Keep, Sink}
import org.ada.server.calc.{Calculator, CalculatorTypePack}
import org.incal.core.akka.AkkaStreamUtil.seqFlow

trait StandardizationCalcTypePack extends CalculatorTypePack {
  type IN = Seq[Option[Double]]
  type OUT = Traversable[Seq[Option[Double]]]
  type INTER = OUT
  type OPT = Seq[(Double, Double)]
  type FLOW_OPT = OPT
  type SINK_OPT = Unit
}

object StandardizationCalc extends Calculator[StandardizationCalcTypePack] {

  override def fun(options: OPT) = _.map(standardizeRow(options))

  override def flow(options: OPT) =
    nonSeqFlow(options).via(seqFlow)

  def nonSeqFlow(options: OPT) =
    Flow[IN].map(standardizeRow(options))

  private def standardizeRow(
    options: OPT)(
    row: IN
  ) =
    row.zip(options).map { case (value, (shift, norm)) =>
      value.map ( value =>
        if (norm != 0) (value - shift) / norm else 0
      )
    }

  override def postFlow(o: Unit) = identity
}