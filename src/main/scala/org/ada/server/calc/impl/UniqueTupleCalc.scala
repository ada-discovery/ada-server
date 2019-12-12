package org.ada.server.calc.impl

import akka.stream.scaladsl.Flow
import org.ada.server.calc.Calculator
import org.incal.core.akka.AkkaStreamUtil._

private class UniqueTupleCalc[A, B] extends Calculator[TupleCalcTypePack[A, B]] {

  private val maxGroups = Int.MaxValue

  override def fun(opt: Unit) =
    _.collect { case (Some(a), Some(b)) => (a, b) }.toSet.toSeq

  override def flow(options: Unit) = {
    val flatFlow = Flow[IN].collect { case (Some(x), Some(y)) => (x, y)}
    flatFlow.via(uniqueFlow[(A, B)](maxGroups)).via(seqFlow)
  }

  override def postFlow(options: Unit) = identity
}

object UniqueTupleCalc {
  def apply[A, B]: Calculator[TupleCalcTypePack[A, B]] = new UniqueTupleCalc[A, B]
}