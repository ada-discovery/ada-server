package org.ada.server.calc.impl

import akka.stream.scaladsl.Flow
import org.ada.server.calc.{Calculator, CalculatorTypePack}
import org.incal.core.akka.AkkaStreamUtil.{groupFlow, seqFlow}
import org.incal.core.util.GroupMapList

trait GroupQuartilesCalcTypePack[G, T] extends CalculatorTypePack {
  type IN = (Option[G], Option[T])
  type OUT = Traversable[(Option[G], Option[Quartiles[T]])]
  type INTER = Traversable[(Option[G], Traversable[T])]
  type OPT = T => Double
  type FLOW_OPT = Unit
  type SINK_OPT = OPT
}

private[calc] class GroupQuartilesCalc[G, T: Ordering] extends Calculator[GroupQuartilesCalcTypePack[G, T]] {

  private val basicCalc = QuartilesCalc.apply[T]
  private val allDefinedBasicCalc = AllDefinedQuartilesCalc.apply[T]

  override def fun(options: OPT) =
    _.toGroupMap.map {
      case (group, values) => (group, basicCalc.fun(options)(values))
    }

  override def flow(options: FLOW_OPT) = {
    val flatFlow = Flow[IN].collect{ case (g, Some(x)) => (g, x) }
    val groupedFlow = flatFlow.via(groupFlow[Option[G], T]())

    groupedFlow.via(seqFlow)
  }

  override def postFlow(options: SINK_OPT) =
    _.map {
      case (group, values) => (group, allDefinedBasicCalc.fun(options)(values))
    }
}

object GroupQuartilesCalc {
  def apply[G, T: Ordering]: Calculator[GroupQuartilesCalcTypePack[G, T]] = new GroupQuartilesCalc[G, T]
}