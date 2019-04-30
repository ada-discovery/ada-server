package org.ada.server.calc.impl

import akka.stream.scaladsl.{Flow, Source}
import org.ada.server.calc.{Calculator, CalculatorTypePack, NoOptionsCalculatorTypePack}
import org.ada.server.akka.AkkaStreamUtil.unzipNFlowsAndApply
import org.ada.server.calc.CalculatorHelper._

trait MultiCalcTypePack[C <: CalculatorTypePack] extends CalculatorTypePack {
  type IN = Seq[C#IN]
  type OUT = Seq[C#OUT]
  type INTER = Seq[C#INTER]
  type OPT = C#OPT
  type FLOW_OPT = C#FLOW_OPT
  type SINK_OPT = C#SINK_OPT
}

private[calc] class MultiAdapterCalc[C <: CalculatorTypePack, MC <: MultiCalcTypePack[C]](coreCalc: Calculator[C]) extends Calculator[MC] {

  override def fun(options: OPT) = { values: Traversable[IN] =>
    val elementsCount = if (values.nonEmpty) values.head.size else 0

    def calcAux(index: Int) = coreCalc.fun(options)(values.map(_(index)))
    (0 until elementsCount).par.map(calcAux).toList
  }

  override def flow(options: FLOW_OPT) = {
    // splitting the seq flow and apply the core flow in parallel for each item
    val seqFlow = (size: Int) => unzipNFlowsAndApply(size)(coreCalc.flow(options))

    // since we need to know the number of features (seq size) we take the first element out, reconcat, apply the flow
    Flow[IN].prefixAndTail(1).flatMapConcat { case (first, tail) =>
      val size = first.headOption.map(_.size).getOrElse(0)
      Source(first).concat(tail).via(seqFlow(size))
    }
  }

  override def postFlow(options: SINK_OPT) = _.map(coreCalc.postFlow(options))
}

object MultiAdapterCalc {

  def apply[C <: CalculatorTypePack](
    coreCalc: Calculator[C]
  ): Calculator[MultiCalcTypePack[C]] = new MultiAdapterCalc[C, MultiCalcTypePack[C]](coreCalc)

  def applyWithType[C <: CalculatorTypePack, MC <: MultiCalcTypePack[C]](
    coreCalc: Calculator[C]
  ): Calculator[MC] = new MultiAdapterCalc[C, MC](coreCalc)
}