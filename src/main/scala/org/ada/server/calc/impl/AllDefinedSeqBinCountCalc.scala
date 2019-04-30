package org.ada.server.calc.impl

import org.ada.server.calc.Calculator

private[calc] class AllDefinedSeqBinCountCalc extends AllDefinedSeqBinCalc[Int, Unit, Int] {

  override protected def getValue(
    values: Seq[Double]
  ) = ()

  override protected def calcAgg(
    values: Traversable[Unit]
  ) = values.size

  override protected def initAccum = 0

  override protected def naAgg = 0

  override protected def updateAccum(
    count: Int,
    value: Unit
  ) = count + 1

  override protected def accumToAgg(
    count: Int
  ) = count
}

object AllDefinedSeqBinCountCalc {
  type AllDefinedSeqBinCountCalcTypePack = AllDefinedSeqBinCalcTypePack[Int, Int]

  def apply: Calculator[AllDefinedSeqBinCountCalcTypePack] = new AllDefinedSeqBinCountCalc
}

private[calc] object SeqBinCountCalcAux extends SeqBinCalc(AllDefinedSeqBinCountCalc.apply)

object SeqBinCountCalc {
  type SeqBinCountCalcTypePack = SeqBinCalcTypePack[Int, Int]

  def apply: Calculator[SeqBinCountCalcTypePack] = SeqBinCountCalcAux
}
