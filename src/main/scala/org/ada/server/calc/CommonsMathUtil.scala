package org.ada.server.calc

import java.{lang => jl}

import org.apache.commons.math3.exception.MaxCountExceededException
import org.apache.commons.math3.special.{Beta, Gamma}
import org.apache.commons.math3.util.{ContinuedFraction, FastMath}

/**
  * Util object/class containing variants of a couple of Apache Commons Math methods.
  */
object CommonsMathUtil {

  /**
    * This is a Scala (optimized-precision) version of the <code>org.apache.commons.math3.special.regularizedGammaP</code> function
    */
  @throws[MaxCountExceededException]
  def regularizedGammaP(
    a: Double,
    x: Double,
    epsilon: Double,
    maxIterations: Int
  ): Option[BigDecimal] =
    if (jl.Double.isNaN(a) || jl.Double.isNaN(x) || (a <= 0.0) || (x < 0.0))
      None
    else if (x == 0.0)
      Some(0d)
    else if (x >= a + 1) {
      // use regularizedGammaQ because it should converge faster in this case.
      val gammaQ: BigDecimal = Gamma.regularizedGammaQ(a, x, epsilon, maxIterations)
      Some(1d - gammaQ)
    } else {
      // calculate series
      var n = 0.0
      // current element index
      var an = 1.0 / a
      // n-th element in the series
      var sum = an // partial sum

      while (FastMath.abs(an / sum) > epsilon && n < maxIterations && sum < Double.PositiveInfinity) {
        // compute next element in the series
        n += 1.0
        an *= x / (a + n)
        // update partial sum
        sum += an
      }

      if (n >= maxIterations)
        throw new MaxCountExceededException(maxIterations)
      else if (jl.Double.isInfinite(sum))
        Some(1d)
      else {
        val result: BigDecimal = FastMath.exp(-x + (a * FastMath.log(x)) - Gamma.logGamma(a)) * sum
        Some(result)
      }
    }

  /**
    * This is a Scala (optimized-precision) version of the <code>org.apache.commons.math3.special.Beta.regularizedBeta</code> function
    */
  @throws[MaxCountExceededException]
  def regularizedBeta(
    x: Double,
    a: Double,
    b: Double,
    epsilon: Double,
    maxIterations: Int
  ): Option[BigDecimal] =
    if (jl.Double.isNaN(x) || jl.Double.isNaN(a) || jl.Double.isNaN(b) || x < 0 || x > 1 || a <= 0 || b <= 0)
      None
    else if (x > (a + 1) / (2 + b + a) && 1 - x <= (b + 1) / (2 + b + a))
      regularizedBeta(1 - x, b, a, epsilon, maxIterations).map ( beta =>
        1d - beta
      )
    else {
      val fraction = new ContinuedFraction() {

        protected def getB(n: Int, x: Double): Double = {
          var ret = .0
          var m = .0
          if (n % 2 == 0) {
            // even
            m = n / 2.0
            ret = (m * (b - m) * x) / ((a + (2 * m) - 1) * (a + (2 * m)))
          }
          else {
            m = (n - 1.0) / 2.0
            ret = -((a + m) * (a + b + m) * x) / ((a + (2 * m)) * (a + (2 * m) + 1.0))
          }
          ret
        }

        protected def getA(n: Int, x: Double) = 1.0
      }
      val result: BigDecimal = FastMath.exp((a * FastMath.log(x)) + (b * FastMath.log1p(-x)) - FastMath.log(a) - Beta.logBeta(a, b)) * 1.0 / fraction.evaluate(x, epsilon, maxIterations)
      Some(result)
    }
}
