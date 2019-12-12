package org.ada.server.calc.impl

import akka.stream.scaladsl.Flow
import org.ada.server.calc.{Calculator, NoOptionsCalculatorTypePack}
import org.incal.core.util.GroupMapList
import org.ada.server.calc.CalculatorHelper._
import org.incal.core.akka.AkkaStreamUtil.seqFlow

trait GroupBasicStatsCalcTypePack[G] extends NoOptionsCalculatorTypePack{
  type IN = (G, Option[Double])
  type OUT = Traversable[(G, Option[BasicStatsResult])]
  type INTER = Traversable[(G, BasicStatsAccum)]
}

private class GroupBasicStatsCalc[G] extends Calculator[GroupBasicStatsCalcTypePack[G]] with GroupBasicStatsHelper {

  private val maxGroups = Int.MaxValue
  private val basicStatsCalc = BasicStatsCalc

  override def fun(o: Unit) =
    _.toGroupMap.map { case (group, values) => (group, basicStatsCalc.fun_(values)) }

  override def flow(o: Unit) = {
    val groupFlow = Flow[IN]
      .groupBy(maxGroups, _._1)
      .map { case (group, value) => group -> initAccum(value)}
      .reduce((l, r) ⇒ (l._1, reduceAccums(l._2, r._2)))
      .mergeSubstreams

    groupFlow.via(seqFlow)
  }

  override def postFlow(o: Unit) =
    _.map { case (group, accum) => (group, basicStatsCalc.postFlow_(accum)) }
}

trait GroupBasicStatsHelper {

  // aux function to initialize an accumulator based on a given value
  def initAccum(value: Option[Double]) =
    value.map( value =>
      BasicStatsAccum(value, value, value, value * value, 1, 0)
    ).getOrElse(
      BasicStatsAccum(Double.MaxValue, Double.MinValue, 0, 0, 0, 1)
    )

  // aux function to reduce accums
  def reduceAccums(accum1: BasicStatsAccum, accum2: BasicStatsAccum) =
    BasicStatsAccum(
      Math.min(accum1.min, accum2.min),
      Math.max(accum1.max, accum2.max),
      accum1.sum + accum2.sum,
      accum1.sqSum + accum2.sqSum,
      accum1.count + accum2.count,
      accum1.undefinedCount + accum2.undefinedCount
    )
}

object GroupBasicStatsCalc {
  def apply[G]: Calculator[GroupBasicStatsCalcTypePack[G]] = new GroupBasicStatsCalc[G]
}