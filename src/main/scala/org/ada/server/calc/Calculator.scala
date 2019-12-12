package org.ada.server.calc

import akka.NotUsed
import akka.stream.scaladsl.Flow
import org.incal.core.akka.AkkaStreamUtil.seqFlow

trait Calculator[C <: CalculatorTypePack] {

  def fun(options: C#OPT): Traversable[C#IN] => C#OUT

  def flow(options: C#FLOW_OPT): Flow[C#IN, C#INTER, NotUsed]

  def postFlow(options: C#SINK_OPT): C#INTER => C#OUT

  // internal type "getters"
  protected type IN = C#IN
  protected type OUT = C#OUT
  protected type INTER = C#INTER
  protected type OPT = C#OPT
  protected type FLOW_OPT = C#FLOW_OPT
  protected type SINK_OPT = C#SINK_OPT
}

trait CalculatorTypePack {
  type IN
  type OUT
  type INTER
  type OPT
  type FLOW_OPT
  type SINK_OPT
}

trait FullDataCalculatorAdapter[C <: FullDataCalculatorTypePack] extends Calculator[C] {

  // need to get all the data so collect
  override def flow(options: C#FLOW_OPT) = seqFlow[C#IN]

  // same as calc
  override def postFlow(options: C#SINK_OPT) = fun(options)
}