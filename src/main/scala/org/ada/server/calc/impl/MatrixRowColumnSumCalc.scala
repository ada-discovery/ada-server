package org.ada.server.calc.impl

import akka.stream.scaladsl.Flow
import org.ada.server.calc.{Calculator, NoOptionsCalculatorTypePack}

trait MatrixRowColumnSumCalcTypePack extends NoOptionsCalculatorTypePack {
  type IN = Seq[Double]
  type OUT = (Seq[Double], Seq[Double])
  type INTER = OUT
}

private object MatrixRowColumnSumCalcAux extends Calculator[MatrixRowColumnSumCalcTypePack] {

  override def fun(o: Unit) = { values: Traversable[IN] =>
    val elementsCount = if (values.nonEmpty) values.head.size else 0

    // summing at the column index
    def columnSum(index: Int) =
      values.foldLeft(0.0) { case (sum, row) => sum + row(index) }

    val rowSums = values.map(_.sum).toSeq
    val columnSums = (0 until elementsCount).par.map(columnSum).toList

    (rowSums, columnSums)
  }

  override def flow(o: Unit) =
    Flow[IN].fold[INTER]((Nil, Nil)) {
      case ((rowSums, columnSums), values) =>
        val initColumnSums = columnSums match {
          case Nil => Seq.fill(values.size)(0d)
          case _ => columnSums
        }

        val newRowSums = rowSums ++ Seq(values.sum)
        val newColumnSums = (initColumnSums, values).zipped.map{_+_}

        (newRowSums, newColumnSums)
    }

  override def postFlow(o: Unit) = identity
}

object MatrixRowColumnSumCalc {
  def apply: Calculator[MatrixRowColumnSumCalcTypePack] = MatrixRowColumnSumCalcAux
}