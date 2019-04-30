package org.ada.server.calc.impl

import org.ada.server.calc.Calculator

object UniqueDistributionCountsCalc {
  type UniqueDistributionCountsCalcTypePack[T] = CountDistinctCalcTypePack[Option[T]]

  def apply[T]: Calculator[UniqueDistributionCountsCalcTypePack[T]] = CountDistinctCalc[Option[T]]
}