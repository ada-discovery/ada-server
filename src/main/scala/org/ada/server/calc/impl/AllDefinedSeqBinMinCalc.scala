package org.ada.server.calc.impl

import org.ada.server.calc.Calculator

private[calc] class AllDefinedSeqBinMinCalc extends AllDefinedSeqBinAggCalc[Option[Double]] {

  override protected def calcAgg(
    values: Traversable[Double]
  ) = if (values.nonEmpty) Some(values.min) else None

  override protected def initAccum = None

  override protected def updateAccum(
    min: Option[Double],
    value: Double
  ) = {
    val newMin = min.map(Math.min(value, _)).getOrElse(value)
    Some(newMin)
  }

  override protected def accumToAgg(
    min: Option[Double]
  ) = min
}

object AllDefinedSeqBinMinCalc {
  type AllDefinedSeqBinMinCalcTypePack = AllDefinedSeqBinCalcTypePack[Option[Double], Option[Double]]

  def apply: Calculator[AllDefinedSeqBinMinCalcTypePack] = new AllDefinedSeqBinMinCalc
}

private[calc] object SeqBinMinCalcAux extends SeqBinCalc(AllDefinedSeqBinMinCalc.apply)

object SeqBinMinCalc {
  type SeqBinMinCalcTypePack = SeqBinCalcTypePack[Option[Double], Option[Double]]

  def apply: Calculator[SeqBinMinCalcTypePack] = SeqBinMinCalcAux
}
