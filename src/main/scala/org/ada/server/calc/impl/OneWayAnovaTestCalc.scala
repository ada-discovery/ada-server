package org.ada.server.calc.impl

import org.apache.commons.math3.exception.MaxCountExceededException
import play.api.Logger
import org.ada.server.calc.{Calculator, NoOptionsCalculatorTypePack}
import org.ada.server.calc.CommonsMathUtil._
import org.ada.server.calc.CalculatorHelper._

trait OneWayAnovaTestCalcTypePack[G] extends NoOptionsCalculatorTypePack{
  type IN = (G, Option[Double])
  type OUT = Option[OneWayAnovaResult]
  type INTER = Traversable[(G, BasicStatsAccum)]
}

private[calc] class OneWayAnovaTestCalc[G] extends Calculator[OneWayAnovaTestCalcTypePack[G]] with OneWayAnovaHelper {

  private val basicStatsCalc = GroupBasicStatsCalc[G]

  override def fun(o: Unit) = { values: Traversable[IN] =>
    val anovaInputs = basicStatsCalc.fun_(values).collect {
      case (_, Some(basicStatsResult)) =>
        OneWayAnovaStatsInputAux(basicStatsResult.sum, basicStatsResult.sqSum, basicStatsResult.definedCount)
    }

    calcAnovaSafe(anovaInputs)
  }

  override def flow(o: Unit) = basicStatsCalc.flow(())

  override def postFlow(o: Unit) = { accums: INTER =>
    val anovaInputs = accums.map { case (_, accum) =>
      OneWayAnovaStatsInputAux(accum.sum, accum.sqSum, accum.count)
    }

    calcAnovaSafe(anovaInputs)
  }
}

trait OneWayAnovaHelper {

  private val epsilon = 1E-100

  protected def calcAnovaFromStats(
    statsResults: Traversable[BasicStatsResult]
  ) = {
    val anovaInputs = statsResults.map (basicStatsResult =>
      OneWayAnovaStatsInputAux(basicStatsResult.sum, basicStatsResult.sqSum, basicStatsResult.definedCount)
    )

    calcAnovaSafe(anovaInputs)
  }

  protected def calcAnovaSafe(
    groups: Traversable[OneWayAnovaStatsInputAux]
  ): Option[OneWayAnovaResult] = {
    val totalCount = groups.map(_.count).sum

    if (groups.size > 1 && (totalCount - groups.size) > 0)
      try {
        calcAnova(groups)
      } catch {
        case _: MaxCountExceededException =>
          Logger.warn(s"Max number of iterations reached for a one-way ANOVA test.")
          None
      }
    else {
      Logger.warn(s"Not enough values to perform a one-way ANOVA test for: ${groups.size} groups.")
      None
    }
  }

  private def calcAnova(
    groups: Traversable[OneWayAnovaStatsInputAux]
  ): Option[OneWayAnovaResult] = {
    val resultAux =
      groups.foldLeft(OneWayAnovaResultAux()) { case (result, data) =>
        val ss = data.sqSum - ((data.sum * data.sum) / data.count)

        OneWayAnovaResultAux(
          result.sum + data.sum,
          result.sqSum + data.sqSum,
          result.count + data.count,
          result.dfwg + data.count - 1,
          result.sswg + ss
        )
      }

    val sst = resultAux.sqSum - ((resultAux.sum * resultAux.sum) / resultAux.count)

    // between group stats
    val dfbg = groups.size - 1
    val msbg = (sst - resultAux.sswg) / dfbg

    // within group stats
    val mswg = resultAux.sswg / resultAux.dfwg

    // F-value
    val FValue = msbg / mswg

    def result(pValue: Double) = OneWayAnovaResult(pValue, FValue, dfbg, resultAux.dfwg)

    // p-value
    if (FValue <= 0)
      Some(result(1d))
    else {
      val beta = regularizedBeta((dfbg * FValue) / (resultAux.dfwg + dfbg * FValue), 0.5 * dfbg, 0.5 * resultAux.dfwg, epsilon, Integer.MAX_VALUE)
      beta.map(beta => result((1d - beta).doubleValue()))
    }
  }

  case class OneWayAnovaStatsInputAux(
    sum: Double,
    sqSum: Double,
    count: Int
  )

  case class OneWayAnovaResultAux(
    sum: Double = 0,
    sqSum: Double = 0,
    count: Double = 0,
    dfwg: Int = 0,
    sswg: Double = 0
  )
}

case class OneWayAnovaResult(
  pValue: Double,
  FValue: Double,
  dfbg: Int, // degree of freedom between groups, degree of freedom in numerator
  dfwg: Int // degree of freedom within groups, degree of freedom in denominator
) extends IndependenceTestResult

object OneWayAnovaTestCalc {
  def apply[G]: Calculator[OneWayAnovaTestCalcTypePack[G]] = new OneWayAnovaTestCalc[G]
}