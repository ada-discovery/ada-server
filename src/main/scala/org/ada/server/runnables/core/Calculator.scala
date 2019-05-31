package org.ada.server.runnables.core

import org.incal.core.runnables.{InputRunnableExt, RunnableHtmlOutput}

class Calculator extends InputRunnableExt[CalculatorSpec] with RunnableHtmlOutput {
  import Operator._

  override def run(input: CalculatorSpec) = {
    // case on the given operator
    val fun: (Double, Double) => Double =
      input.operator match {
        case Plus => _ + _
        case Minus => _ - _
        case Multiply => _ * _
        case Divide => _ / _
      }

    addParagraph(s"${input.a} ${input.operator} ${input.b} = ${bold(fun(input.a, input.b).toString)}")
  }
}

object Operator extends Enumeration {
  val Plus = Value("+")
  val Minus = Value("-")
  val Multiply = Value("*")
  val Divide = Value("/")
}

case class CalculatorSpec(
  a: Double,
  operator: Operator.Value,
  b: Double
)