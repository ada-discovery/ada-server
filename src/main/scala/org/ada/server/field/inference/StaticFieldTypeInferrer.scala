package org.ada.server.field.inference

import akka.stream.scaladsl.Flow
import org.ada.server.calc.Calculator
import org.ada.server.dataaccess.AdaConversionException
import org.ada.server.field.FieldType
import play.api.libs.json.JsReadable

trait StaticFieldTypeInferrerTypePack[T] extends SingleFieldTypeInferrerTypePack[T] {
  type INTER = Boolean
}

private trait StaticFieldTypeInferrerImpl[T] extends Calculator[StaticFieldTypeInferrerTypePack[T]] {

  protected val fieldType: FieldType[_]

  override def fun(o: Unit) = { values: Traversable[IN] =>
    val passed = values.view.map(check).forall(identity)

    if (passed) Some(fieldType) else None
  }

  override def flow(o: Unit) =
    Flow[IN].fold[INTER](true) {
      case (passed, text) => passed & check(text)
    }

  override def postFlow(o: Unit) =
    (passed) => if (passed) Some(fieldType) else None

  private[inference] def check(text: T): Boolean
}

private final class StringStaticFieldTypeInferrer(val fieldType: FieldType[_]) extends StaticFieldTypeInferrerImpl[String] {

  override private[inference] def check(text: String): Boolean =
    try {
      fieldType.displayStringToValue(text)
      true
    } catch {
      case _: AdaConversionException => false
    }
}

private final class JsonStaticFieldTypeInferrer(val fieldType: FieldType[_]) extends StaticFieldTypeInferrerImpl[JsReadable] {

  override private[inference] def check(json: JsReadable): Boolean =
    try {
      fieldType.displayJsonToValue(json)
      true
    } catch {
      case _: AdaConversionException => false
    }
}

object StaticFieldTypeInferrer {

  type of[T] = Calculator[StaticFieldTypeInferrerTypePack[T]]

  def ofString(fieldType: FieldType[_]): of[String] =
    new StringStaticFieldTypeInferrer(fieldType)

  def ofJson(fieldType: FieldType[_]): of[JsReadable] =
    new JsonStaticFieldTypeInferrer(fieldType)
}