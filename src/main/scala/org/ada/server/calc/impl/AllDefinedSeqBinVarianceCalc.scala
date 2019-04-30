package org.ada.server.calc.impl

import org.ada.server.calc.Calculator

private[calc] class AllDefinedSeqBinVarianceCalc extends AllDefinedSeqBinAggCalc[(Double, Double, Int)] {

  override protected def calcAgg(
    values: Traversable[Double]
  ) = if (values.nonEmpty) {
    val count = values.size
    val mean = values.sum / count
    val meanSq = values.foldLeft(0.0)((accum, value) => accum + value * value) / count
    Some(meanSq - mean * mean)
  } else None

  override protected def initAccum = (0d, 0d, 0)

  override protected def updateAccum(
    accum: (Double, Double, Int),
    value: Double
  ) = (accum._1 + value, accum._2 + value * value, accum._3 + 1)

  override protected def accumToAgg(
    accum: (Double, Double, Int)
  ) = if (accum._3 > 0) {
    val count = accum._3
    val mean = accum._1 / count
    val meanSq = accum._2 / count
    Some(meanSq - mean * mean)
  } else
    None
}

object AllDefinedSeqBinVarianceCalc {
  type AllDefinedSeqBinVarianceCalcTypePack = AllDefinedSeqBinCalcTypePack[(Double, Double, Int), Option[Double]]

  def apply: Calculator[AllDefinedSeqBinVarianceCalcTypePack] = new AllDefinedSeqBinVarianceCalc
}

private[calc] object SeqBinVarianceCalcAux extends SeqBinCalc(AllDefinedSeqBinVarianceCalc.apply)

object SeqBinVarianceCalc {
  type SeqBinVarianceCalcTypePack = SeqBinCalcTypePack[(Double, Double, Int), Option[Double]]

  def apply: Calculator[SeqBinVarianceCalcTypePack] = SeqBinVarianceCalcAux
}
