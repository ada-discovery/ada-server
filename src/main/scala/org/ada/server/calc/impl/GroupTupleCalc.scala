package org.ada.server.calc.impl

import akka.stream.scaladsl.{Flow, Keep, Sink}
import org.ada.server.calc.{Calculator, NoOptionsCalculatorTypePack}
import org.incal.core.akka.AkkaStreamUtil._
import org.incal.core.util.GroupMapList3

trait GroupTupleCalcTypePack[G, A, B] extends NoOptionsCalculatorTypePack {
  type IN = (Option[G], Option[A], Option[B])
  type OUT = Traversable[(Option[G], Traversable[(A, B)])]
  type INTER = Seq[(Option[G], Seq[(A, B)])]
}

private[calc] class GroupTupleCalc[G, A, B] extends Calculator[GroupTupleCalcTypePack[G, A, B]] {

  override def fun(opt: Unit)  =
    _.toGroupMap.map {
      case (groupValue, values) => (groupValue, values.flatMap(toOption))
    }

  override def flow(options: Unit) = {
    val flatFlow = Flow[IN].collect{ case (g, Some(x), Some(y)) => (g, (x, y)) }
    val groupedFlow = flatFlow.via(groupFlow[Option[G], (A, B)]())

    groupedFlow.via(seqFlow)
  }

  override def postFlow(options: Unit) = identity

  private def toOption(ab: (Option[A], Option[B])) =
    ab._1.flatMap(a =>
      ab._2.map(b => (a, b))
    )
}

object GroupTupleCalc {
  def apply[G, A, B]: Calculator[GroupTupleCalcTypePack[G, A, B]] = new GroupTupleCalc[G, A, B]
}