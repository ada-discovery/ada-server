package org.ada.server.calc.impl

import akka.stream.scaladsl.{Flow, Source}
import org.ada.server.calc.{Calculator, NoOptionsCalculatorTypePack}
import org.ada.server.calc.CalculatorHelper._
import org.incal.core.akka.AkkaStreamUtil.unzipNFlowsAndApply

trait MultiChiSquareTestCalcTypePack[G, T] extends NoOptionsCalculatorTypePack {
  type IN = (G, Seq[Option[T]])
  type OUT = Seq[Option[ChiSquareResult]]
  type INTER =  Seq[ChiSquareTestCalcTypePack[G, Option[T]]#INTER]
}

private[calc] class MultiChiSquareTestCalc[G, T] extends Calculator[MultiChiSquareTestCalcTypePack[G, T]] {

  private val coreCalc = ChiSquareTestCalc[G, Option[T]]

  override def fun(o: Unit) = { values: Traversable[IN] =>
    if (values.isEmpty || values.head._2.isEmpty)
      Nil
    else {
      val testsNum = values.head._2.size
      (0 to testsNum - 1).map { index =>
        val inputs = values.map(row => (row._1, row._2(index)))
        coreCalc.fun_(inputs)
      }
    }
  }

  override def flow(o: Unit) = {
    // create tuples for each value
    val tupleSeqFlow = Flow[IN].map { case (group, values) => values.map((group, _))}

    // apply the core flow, i.e., a single chi-square test in parallel by splitting the flow
    val seqChiSquareFlow = (size: Int) => unzipNFlowsAndApply(size)(coreCalc.flow_)

    // since we need to know the number of features (seq size) we take the first element out, apply the flow and concat
    tupleSeqFlow.prefixAndTail(1).flatMapConcat { case (first, tail) =>
      val size = first.headOption.map(_.size).getOrElse(0)
      Source(first).concat(tail).via(seqChiSquareFlow(size))
    }
  }

  override def postFlow(o: Unit) =
    _.map(coreCalc.postFlow_(_))
}

object MultiChiSquareTestCalc {
  def apply[G, T]: Calculator[MultiChiSquareTestCalcTypePack[G, T]] = new MultiChiSquareTestCalc[G, T]
}