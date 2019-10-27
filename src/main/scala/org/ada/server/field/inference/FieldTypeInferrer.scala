package org.ada.server.field.inference

import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.ada.server.calc.{Calculator, CalculatorHelper, NoOptionsCalculatorTypePack}
import org.ada.server.field._
import play.api.libs.json.JsReadable
import CalculatorHelper.NoOptionsExt

trait FieldTypeInferrerTypePack[T] extends NoOptionsCalculatorTypePack{
  type IN = T
  type OUT = FieldType[_]
  type INTER = Seq[Any]
}

trait FieldTypeInferrer[T] extends Calculator[FieldTypeInferrerTypePack[T]] {

  // short-hand for convenient execution
  def apply(values: Traversable[T]) = fun()(values)

  // short-hand for convenient execution
  def apply(
    source: Source[T, _])(
    implicit materializer: Materializer
  ) = this.runFlow_(source)
}

class FieldTypeInferrerFactory(
  ftf: FieldTypeFactory,
  maxEnumValuesCount: Int,
  minAvgValuesPerEnum: Double,
  arrayDelimiter: String
) {

  def ofString: FieldTypeInferrer[String] =
    new DisplayStringFieldTypeInferrerImpl(ftf, maxEnumValuesCount, minAvgValuesPerEnum, arrayDelimiter)

  def ofJson: FieldTypeInferrer[JsReadable] =
    new DisplayJsonFieldTypeInferrerImpl(ftf, maxEnumValuesCount, minAvgValuesPerEnum, arrayDelimiter)
}