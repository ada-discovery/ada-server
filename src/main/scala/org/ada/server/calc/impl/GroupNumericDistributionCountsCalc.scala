package org.ada.server.calc.impl

import akka.stream.scaladsl.Flow
import org.ada.server.calc.{Calculator, CalculatorTypePack}
import org.incal.core.util.GroupMapList
import org.incal.core.akka.AkkaStreamUtil._

trait GroupNumericDistributionCountsCalcTypePack[G] extends CalculatorTypePack {
  type IN = (Option[G], Option[Double])
  type OUT = Traversable[(Option[G], Traversable[(BigDecimal, Int)])]
  type INTER = Traversable[((Option[G], Int), Int)]
  type OPT = NumericDistributionOptions
  type FLOW_OPT = NumericDistributionFlowOptions
  type SINK_OPT = FLOW_OPT
}

private[calc] class GroupNumericDistributionCountsCalc[G] extends Calculator[GroupNumericDistributionCountsCalcTypePack[G]] with NumericDistributionCountsHelper {

  private val maxGroups = Int.MaxValue
  private val normalCalc = NumericDistributionCountsCalc.apply

  override def fun(options: OPT) =
    _.toGroupMap.map { case (group, values) =>
      (group, normalCalc.fun(options)(values))
    }

  override def flow(options: FLOW_OPT) = {
    val stepSize = calcStepSize(
      options.binCount,
      options.min,
      options.max,
      options.specialBinForMax
    )

    val minBg = BigDecimal(options.min)
    val max = options.max

    val flatFlow = Flow[IN].collect { case (g, Some(x)) => (g, x)}

    val groupBucketIndexFlow = Flow[(Option[G], Double)]
      .groupBy(maxGroups, _._1)
      .map { case (group, value) =>
        group -> calcBucketIndex(
          stepSize, options.binCount, minBg, max)(value)
      }.mergeSubstreams

    flatFlow.via(groupBucketIndexFlow).via(countFlow(maxGroups)).via(seqFlow)
  }

  override def postFlow(options: FLOW_OPT) = { elements =>
    val stepSize = calcStepSize(
      options.binCount,
      options.min,
      options.max,
      options.specialBinForMax
    )

    val minBg = BigDecimal(options.min)

    val groupIndexCounts = elements.map { case ((group, index), count) => (group, (index, count))}.toGroupMap

    groupIndexCounts.map { case (group, counts) =>
      val indexCountMap = counts.toMap

      val xValueCounts =
        for (index <- 0 to options.binCount - 1) yield {
          val count = indexCountMap.get(index).getOrElse(0)
          val xValue = minBg + (index * stepSize)
          (xValue, count)
        }
      (group, xValueCounts)
    }
  }
}

object GroupNumericDistributionCountsCalc {
  def apply[G]: Calculator[GroupNumericDistributionCountsCalcTypePack[G]] = new GroupNumericDistributionCountsCalc[G]
}