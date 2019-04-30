package org.ada.server.calc.impl

import org.ada.server.calc.Calculator

private[calc] class AllDefinedSeqBinMaxCalc extends AllDefinedSeqBinAggCalc[Option[Double]] {

  override protected def calcAgg(
    values: Traversable[Double]
  ) = if (values.nonEmpty) Some(values.max) else None

  override protected def initAccum = None

  override protected def updateAccum(
    max: Option[Double],
    value: Double
  ) = {
    val newMax = max.map(Math.max(value, _)).getOrElse(value)
    Some(newMax)
  }

  override protected def accumToAgg(
    max: Option[Double]
  ) = max
}

object AllDefinedSeqBinMaxCalc {
  type AllDefinedSeqBinMaxCalcTypePack = AllDefinedSeqBinCalcTypePack[Option[Double], Option[Double]]

  def apply: Calculator[AllDefinedSeqBinMaxCalcTypePack] = new AllDefinedSeqBinMaxCalc
}

private[calc] object SeqBinMaxCalcAux extends SeqBinCalc(AllDefinedSeqBinMaxCalc.apply)

object SeqBinMaxCalc {
  type SeqBinMaxCalcTypePack = SeqBinCalcTypePack[Option[Double], Option[Double]]

  def apply: Calculator[SeqBinMaxCalcTypePack] = SeqBinMaxCalcAux
}
