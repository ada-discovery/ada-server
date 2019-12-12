package org.ada.server.calc.impl

import akka.stream.scaladsl.Flow
import org.ada.server.calc.impl.UniqueDistributionCountsCalc.UniqueDistributionCountsCalcTypePack
import org.ada.server.calc.{Calculator, NoOptionsCalculatorTypePack}
import org.incal.core.akka.AkkaStreamUtil.{countFlow, seqFlow}

trait CumulativeOrderedCountsCalcTypePack[T] extends NoOptionsCalculatorTypePack {
  type IN = Option[T]
  type OUT = Traversable[(T, Int)]
  type INTER = OUT
}

private class CumulativeOrderedCountsCalc[T: Ordering] extends Calculator[CumulativeOrderedCountsCalcTypePack[T]] with CumulativeOrderedCountsCalcFun {

  private val maxGroups = Int.MaxValue

  private val basicCalc = UniqueDistributionCountsCalc.apply[T]

  override def fun(options: Unit): Traversable[IN] => OUT =
    (basicCalc.fun(())(_)) andThen sortAndCount[T]

  override def flow(options: Unit) = {
    val flatFlow = Flow[IN].collect { case Some(x) => x }
    flatFlow.via(countFlow[T](maxGroups)).via(seqFlow) map sortAndCountFlow[T]
  }

  override def postFlow(options: Unit) = identity
}

trait CumulativeOrderedCountsCalcFun {

  protected def sortAndCount[T: Ordering](values: UniqueDistributionCountsCalcTypePack[T]#OUT) = {
    val ordered = values.toSeq.collect { case (Some(value), count) => (value, count) }.sortBy(_._1)
    val sums = ordered.scanLeft(0: Int) { case (sum, (_, count)) => count + sum }
    sums.tail.zip(ordered).map { case (sum, (value, _)) => (value, sum) }
  }

  protected def sortAndCountFlow[T: Ordering](values: Traversable[(T, Int)]) = {
    val ordered = values.toSeq.sortBy(_._1)
    val sums = ordered.scanLeft(0: Int) { case (sum, (_, count)) => count + sum }
    sums.tail.zip(ordered).map { case (sum, (value, _)) => (value, sum) }
  }
}

object CumulativeOrderedCountsCalc {
  def apply[T: Ordering]: Calculator[CumulativeOrderedCountsCalcTypePack[T]] = new CumulativeOrderedCountsCalc[T]
}