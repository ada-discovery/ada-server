package org.ada.server.calc.impl

import akka.stream.scaladsl.Flow
import org.ada.server.calc.{Calculator, CalculatorTypePack}

private[calc] abstract class SeqOptionInputCalc[C <: CalculatorTypePack](val allDefinedCalc: Calculator[C]) {

  protected type INN

  protected def toAllDefined: Seq[INN] => C#IN

  def fun(options: C#OPT) =
    (values: Traversable[Seq[Option[INN]]]) => {
      val flatValues = values.collect { case row: Seq[Option[INN]] if row.forall(_.isDefined) => toAllDefined(row.flatten) }
      allDefinedCalc.fun(options)(flatValues)
    }

  def flow(options: C#FLOW_OPT) = {
    val allDefinedFlow = allDefinedCalc.flow(options)
    val flatFlow = Flow[Seq[Option[INN]]].collect { case row: Seq[Option[INN]] if row.forall(_.isDefined) => toAllDefined(row.flatten) }
    flatFlow.via(allDefinedFlow)
  }

  def postFlow(options: C#SINK_OPT) =
    allDefinedCalc.postFlow(options)
}