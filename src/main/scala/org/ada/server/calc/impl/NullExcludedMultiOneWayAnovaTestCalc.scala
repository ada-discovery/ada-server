package org.ada.server.calc.impl

import akka.stream.scaladsl.Flow
import org.ada.server.calc.{Calculator, NoOptionsCalculatorTypePack}
import org.ada.server.calc.CalculatorHelper._

trait NullExcludedMultiOneWayAnovaTestCalcTypePack[G] extends NoOptionsCalculatorTypePack{
  type IN = (Option[G], Seq[Option[Double]])
  type OUT = MultiOneWayAnovaTestCalcTypePack[G]#OUT
  type INTER = MultiOneWayAnovaTestCalcTypePack[G]#INTER
}

private[calc] class NullExcludedMultiOneWayAnovaTestCalc[G] extends Calculator[NullExcludedMultiOneWayAnovaTestCalcTypePack[G]] with OneWayAnovaHelper {

  private val coreCalc = MultiOneWayAnovaTestCalc[G]

  override def fun(o: Unit) = { values: Traversable[IN] =>
    val groupDefinedValues = values.collect { case (Some(group), values) => (group, values) }
    coreCalc.fun_(groupDefinedValues)
  }

  override def flow(o: Unit) = {
    // flatten the flow by removing undefined group
    val flatFlow = Flow[IN].collect { case (Some(group), values ) => (group, values) }

    flatFlow.via(coreCalc.flow_)
  }

  override def postFlow(o: Unit) =
    coreCalc.postFlow_
}

object NullExcludedMultiOneWayAnovaTestCalc {
  def apply[G]: Calculator[NullExcludedMultiOneWayAnovaTestCalcTypePack[G]] = new NullExcludedMultiOneWayAnovaTestCalc[G]
}