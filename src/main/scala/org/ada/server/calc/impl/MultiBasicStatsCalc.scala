package org.ada.server.calc.impl

import akka.stream.scaladsl.{Flow, Sink}
import org.ada.server.calc.{Calculator, NoOptionsCalculatorTypePack}

trait MultiBasicStatsCalcTypePack extends NoOptionsCalculatorTypePack {
  type IN = Seq[Option[Double]]
  type OUT = Seq[Option[BasicStatsResult]]
  type INTER = Seq[BasicStatsAccum]
}

object MultiBasicStatsCalc extends Calculator[MultiBasicStatsCalcTypePack] {

  private val basicCalc = BasicStatsCalc

  override def fun(o: Unit) = { values: Traversable[IN] =>
    val elementsCount = if (values.nonEmpty) values.head.size else 0

    def calcAux(index: Int) = basicCalc.fun()(values.map(_(index)))

    (0 until elementsCount).par.map(calcAux).toList
  }

  override def flow(o: Unit) =
    Flow[IN].fold[INTER](Nil) {
      case (accums, values) =>

        // init accumulators if needed
        val initAccums = accums match {
          case Nil => Seq.fill(values.size)(BasicStatsAccum(Double.MaxValue, Double.MinValue, 0, 0, 0, 0))
          case _ => accums
        }

        initAccums.zip(values).map { case (accum, value) => basicCalc.updateAccum(accum, value)}
    }

  override def postFlow(o: Unit) = _.map(basicCalc.postFlow(()))
}