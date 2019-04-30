package org.ada.server.calc.impl

private[impl] trait AllDefinedSeqBinAggCalc[ACCUM] extends AllDefinedSeqBinCalc[ACCUM, Double, Option[Double]] {
  override protected def getValue(values: Seq[Double]) = values.last
  override protected def naAgg = None
}