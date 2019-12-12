package org.ada.server.calc.impl

import akka.stream.scaladsl.Flow
import org.ada.server.calc.Calculator
import org.incal.core.akka.AkkaStreamUtil._
import org.incal.core.util.GroupMapList3

private[calc] class GroupUniqueTupleCalc[G, A, B] extends Calculator[GroupTupleCalcTypePack[G, A, B]] {

  private val maxGroups = Int.MaxValue

  private val basicCalc = UniqueTupleCalc[A, B]

  override def fun(opt: Unit)  =
    _.toGroupMap.map {
      case (groupValue, values) => (groupValue, basicCalc.fun(())(values))
    }

  override def flow(options: Unit) = {
    val flatFlow = Flow[IN].collect{ case (g, Some(a), Some(b)) => (g, (a, b)) }

    val uniqueGroupedFlow = flatFlow.via(
      uniqueFlow[(Option[G], (A, B))](maxGroups)
    ).via(
      groupFlow[Option[G], (A, B)](maxGroups)
    )
    uniqueGroupedFlow.via(seqFlow)
  }

  override def postFlow(options: Unit) = identity
}

object GroupUniqueTupleCalc {
  def apply[G, A, B]: Calculator[GroupTupleCalcTypePack[G, A, B]] = new GroupUniqueTupleCalc[G, A, B]
}