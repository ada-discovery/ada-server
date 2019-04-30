package org.ada.server.calc.impl

import org.ada.server.calc.{Calculator, NoOptionsCalculatorTypePack}

object MultiCountDistinctCalc {

  type MultiCountDistinctCalcTypePack[T] =
    MultiCalcTypePack[CountDistinctCalcTypePack[T]] with NoOptionsCalculatorTypePack

  def apply[T]: Calculator[MultiCountDistinctCalcTypePack[T]] =
    MultiAdapterCalc.applyWithType(CountDistinctCalc[T])
}