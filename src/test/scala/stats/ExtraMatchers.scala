package stats

import org.scalatest.Matchers

trait ExtraMatchers { _: Matchers =>

  implicit class ShouldBeAround(value1: Double) {
    def shouldBeAround(value2: Double, precision: Double) = {
      Math.abs(value1 - value2) should be < (precision)
    }
  }
}
