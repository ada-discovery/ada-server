package org.ada.server.calc.impl

import org.ada.server.calc.Calculator

private[calc] class AllDefinedSeqBinMeanCalc extends AllDefinedSeqBinAggCalc[(Double, Int)] {

  override protected def calcAgg(
    values: Traversable[Double]
  ) = if (values.nonEmpty) Some(values.sum / values.size) else None

  override protected def initAccum = (0d, 0)

  override protected def updateAccum(
    accum: (Double, Int),
    value: Double
  ) = (accum._1 + value, accum._2 + 1)

  override protected def accumToAgg(
    accum: (Double, Int)
  ) = if (accum._2 > 0) Some(accum._1 / accum._2) else None
}

object AllDefinedSeqBinMeanCalc {
  type AllDefinedSeqBinMeanCalcTypePack = AllDefinedSeqBinCalcTypePack[(Double, Int), Option[Double]]

  def apply: Calculator[AllDefinedSeqBinMeanCalcTypePack] = new AllDefinedSeqBinMeanCalc
}

private[calc] object SeqBinMeanCalcAux extends SeqBinCalc(AllDefinedSeqBinMeanCalc.apply)

object SeqBinMeanCalc {
  type SeqBinMeanCalcTypePack = SeqBinCalcTypePack[(Double, Int), Option[Double]]

  def apply: Calculator[SeqBinMeanCalcTypePack] = SeqBinMeanCalcAux
}
