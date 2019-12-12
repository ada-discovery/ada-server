package org.ada.server.calc.impl

import akka.stream.scaladsl.{Flow, Source}
import org.ada.server.calc.{Calculator, NoOptionsCalculatorTypePack}
import org.ada.server.calc.CalculatorHelper._
import org.incal.core.akka.AkkaStreamUtil.unzipNFlowsAndApply

trait NullExcludedMultiChiSquareTestCalcTypePack[G, T] extends NoOptionsCalculatorTypePack {
  type IN = (Option[G], Seq[Option[T]])
  type OUT = Seq[Option[ChiSquareResult]]
  type INTER =  Seq[ChiSquareTestCalcTypePack[G, T]#INTER]
}

private[calc] class NullExcludedMultiChiSquareTestCalc[G, T] extends Calculator[NullExcludedMultiChiSquareTestCalcTypePack[G, T]] {

  private val coreCalc = ChiSquareTestCalc[G, T]

  override def fun(o: Unit) = { values: Traversable[IN] =>
    if (values.isEmpty || values.head._2.isEmpty)
      Nil
    else {
      val groupDefinedValues = values.collect { case (Some(group), values) => (group, values) }
      val testsNum = values.head._2.size
      (0 to testsNum - 1).map { index =>
        val inputs = groupDefinedValues.flatMap(row => row._2(index).map((row._1, _)))
        coreCalc.fun_(inputs)
      }
    }
  }

  override def flow(o: Unit) = {
    // create tuples for each value
    val tupleSeqFlow = Flow[IN].map { case (group, values) => values.map((group, _))}

    // flatten the flow by removing undefined values
    val flatFlow = Flow[(Option[G], Option[T])].collect { case (Some(group), Some(value)) => (group, value) }

    // apply core flow for a single chi-square test in parallel by splitting the flow
    val seqChiSquareFlow = (size: Int) => unzipNFlowsAndApply(size)(flatFlow.via(coreCalc.flow_))

    // since we need to know the number of features (seq size) we take the first element out, apply the flow and concat
    tupleSeqFlow.prefixAndTail(1).flatMapConcat { case (first, tail) =>
      val size = first.headOption.map(_.size).getOrElse(0)
      Source(first).concat(tail).via(seqChiSquareFlow(size))
    }
  }

  override def postFlow(o: Unit) =
    _.map(coreCalc.postFlow_(_))
}

object NullExcludedMultiChiSquareTestCalc {
  def apply[G, T]: Calculator[NullExcludedMultiChiSquareTestCalcTypePack[G, T]] = new NullExcludedMultiChiSquareTestCalc[G, T]
}