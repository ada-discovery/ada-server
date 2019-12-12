package org.ada.server.calc.impl

import akka.stream.scaladsl.Flow
import org.ada.server.calc.{Calculator, NoOptionsCalculatorTypePack}
import org.incal.core.akka.AkkaStreamUtil.{countFlow, seqFlow}
import org.incal.core.util.GroupMapList

trait GroupCumulativeOrderedCountsCalcTypePack[G, T] extends NoOptionsCalculatorTypePack {
  type IN = (Option[G], Option[T])
  type OUT = Traversable[(Option[G], Traversable[(T, Int)])]
  type INTER = Traversable[((Option[G], T), Int)]
}

private class GroupCumulativeOrderedCountsCalc[G, T: Ordering] extends Calculator[GroupCumulativeOrderedCountsCalcTypePack[G, T]] with CumulativeOrderedCountsCalcFun {

  private val maxGroups = Int.MaxValue

  private val basicCalc = GroupUniqueDistributionCountsCalc.apply[G, T]

  override def fun(options: Unit) =
    (basicCalc.fun(options)(_)) andThen groupSortAndCount

  override def flow(options: Unit) = {
    val flatFlow = Flow[IN].collect { case (g, Some(x)) => (g, x) }
    flatFlow.via(countFlow[(Option[G], T)](maxGroups)).via(seqFlow)
  }

  override def postFlow(options: Unit) = (values) =>
    groupSortAndCountFlow(
      values.map { case ((group, value), count) => (group, (value, count)) }.toGroupMap
    )

  private def groupSortAndCount(groupValues: GroupUniqueDistributionCountsCalcTypePack[G, T]#OUT) =
    groupValues.map { case (group, values) => (group, sortAndCount(values)) }

  private def groupSortAndCountFlow(groupValues:  OUT) =
    groupValues.map { case (group, values) => (group, sortAndCountFlow(values)) }
}

object GroupCumulativeOrderedCountsCalc {
  def apply[G, T: Ordering]: Calculator[GroupCumulativeOrderedCountsCalcTypePack[G, T]] = new GroupCumulativeOrderedCountsCalc[G, T]
}