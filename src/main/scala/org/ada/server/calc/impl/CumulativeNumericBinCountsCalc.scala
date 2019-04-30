package org.ada.server.calc.impl

import org.ada.server.calc.Calculator

trait CumulativeNumericBinCountsCalcTypePack extends NumericDistributionCountsCalcTypePack

private[calc] class CumulativeNumericBinCountsCalc extends Calculator[CumulativeNumericBinCountsCalcTypePack] with CumulativeNumericBinCountsCalcFun {

  private val basicCalc = NumericDistributionCountsCalc.apply

  override def fun(options: OPT) =
    (basicCalc.fun(options)(_)) andThen sortAndCount

  override def flow(options: FLOW_OPT) =
    basicCalc.flow(options)

  override def postFlow(options: SINK_OPT) =
    basicCalc.postFlow(options) andThen sortAndCount
}

trait CumulativeNumericBinCountsCalcFun {

  protected def sortAndCount(values: NumericDistributionCountsCalcTypePack#OUT) = {
    val ordered = values.toSeq.sortBy(_._1)
    val sums = ordered.scanLeft(0: Int) { case (sum, (_, count)) => count + sum }
    sums.tail.zip(ordered).map { case (sum, (value, _)) => (value, sum) }
  }
}

object CumulativeNumericBinCountsCalc {
  def apply: Calculator[CumulativeNumericBinCountsCalcTypePack] = new CumulativeNumericBinCountsCalc
}