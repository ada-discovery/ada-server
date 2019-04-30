package org.ada.server.calc.impl

import org.ada.server.calc.{Calculator, FullDataCalculatorAdapter, FullDataCalculatorTypePack}

trait AllDefinedQuartilesCalcTypePack[T] extends FullDataCalculatorTypePack {
  type IN = T
  type OUT = Option[Quartiles[T]]
  type OPT = T => Double
}

private class AllDefinedQuartilesCalc[T: Ordering] extends FullDataCalculatorAdapter[AllDefinedQuartilesCalcTypePack[T]] {

  /**
    * Calculate quartiles for boxplots.
    * Generation is meant for Tukey boxplots.
    *
    * @param elements sequence of elements.
    * @return 5-value tuple with (lower 1.5 IQR whisker, lower quartile, median, upper quartile, upper 1.5 IQR whisker)
    */
  override def fun(toDouble: T => Double)  = { elements =>
    elements.headOption.map { _ =>
      val sorted = elements.toSeq.sorted
      val length = sorted.size

      // median
      val median = sorted(length / 2)

      // upper quartile
      val upperQuartile = sorted(3 * length / 4)

      // lower quartile
      val lowerQuartile = sorted(length / 4)

      val upperQuartileDouble = toDouble(upperQuartile)
      val lowerQuartileDouble = toDouble(lowerQuartile)
      val iqr = upperQuartileDouble - lowerQuartileDouble

      val upperWhiskerValue = upperQuartileDouble + 1.5 * iqr
      val lowerWhiskerValue = lowerQuartileDouble - 1.5 * iqr

      val lowerWhisker = sorted.find(value => toDouble(value) >= lowerWhiskerValue).getOrElse(sorted.last)
      val upperWhisker = sorted.reverse.find(value => toDouble(value) <= upperWhiskerValue).getOrElse(sorted.head)

      Quartiles(lowerWhisker, lowerQuartile, median, upperQuartile, upperWhisker)
    }
  }
}

case class Quartiles[T <% Ordered[T]](
    lowerWhisker: T,
    lowerQuantile: T,
    median: T,
    upperQuantile: T,
    upperWhisker: T
  ) {
    def ordering = implicitly[Ordering[T]]
    def toSeq: Seq[T] = Seq(lowerWhisker, lowerQuantile, median, upperQuantile, upperWhisker)
}

object AllDefinedQuartilesCalc {
  def apply[T: Ordering]: Calculator[AllDefinedQuartilesCalcTypePack[T]] = new AllDefinedQuartilesCalc[T]
}