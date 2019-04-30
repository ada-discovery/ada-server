package org.ada.server.calc.impl

import org.ada.server.calc.Calculator

trait GroupCumulativeNumericBinCountsCalcTypePack[G] extends GroupNumericDistributionCountsCalcTypePack[G]

private[calc] class GroupCumulativeNumericBinCountsCalc[G] extends Calculator[GroupCumulativeNumericBinCountsCalcTypePack[G]] with CumulativeNumericBinCountsCalcFun {

  private val basicCalc = GroupNumericDistributionCountsCalc.apply[G]

  override def fun(options: OPT) =
    (basicCalc.fun(options)(_)) andThen groupSortAndCount

  override def flow(options: FLOW_OPT) =
    basicCalc.flow(options)

  override def postFlow(options: SINK_OPT) =
    basicCalc.postFlow(options) andThen groupSortAndCount

  private def groupSortAndCount(groupValues: GroupNumericDistributionCountsCalcTypePack[G]#OUT) =
    groupValues.map { case (group, values) => (group, sortAndCount(values)) }
}

object GroupCumulativeNumericBinCountsCalc {
  def apply[G]: Calculator[GroupCumulativeNumericBinCountsCalcTypePack[G]] = new GroupCumulativeNumericBinCountsCalc[G]
}