package org.ada.server.field.inference

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import org.ada.server.field._
import play.api.libs.json.JsReadable

import scala.concurrent.Future

trait FieldTypeInferrer[T] {
  def apply(values: Traversable[T]): FieldType[_]

  def apply(
    values: Source[T, _])(
    implicit materializer: Materializer
  ): Future[FieldType[_]]
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