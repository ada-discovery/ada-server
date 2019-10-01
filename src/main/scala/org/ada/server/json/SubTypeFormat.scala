package org.ada.server.json

import play.api.libs.json._

import scala.reflect.ClassTag

case class RuntimeClassFormat[E](
  format: Format[E],
  runtimeClass: Class[E]
)

object RuntimeClassFormat {

  def apply[E](
    format: Format[E])(
    implicit tag: ClassTag[E]
  ): RuntimeClassFormat[E] =
    RuntimeClassFormat(format, tag.runtimeClass.asInstanceOf[Class[E]])
}

abstract class HasFormat[E](implicit tag: ClassTag[E]) {
  val format: Format[E]
  val runtimeClass = tag.runtimeClass.asInstanceOf[Class[E]]
}

class SubTypeFormat[T](formats: Traversable[RuntimeClassFormat[_ <: T]]) extends Format[T] {

  private val concreteClassFieldName = "concreteClass"

  private val classNameFormatMap = formats.map(classAndFormat =>
    (
      classAndFormat.runtimeClass.getName,
      classAndFormat.format.asInstanceOf[Format[T]]
    )
  ).toMap

  override def reads(json: JsValue): JsResult[T] = {
    val concreteClassName = (json \ concreteClassFieldName).asOpt[String].getOrElse(
      throw new IllegalArgumentException(s"Field '$concreteClassFieldName' cannot be found in $json.")
    )

    val format = classNameFormatMap.getOrElse(concreteClassName,
      throw new IllegalArgumentException(s"Json Formatter for a sub type '$concreteClassName' not recognized."))

    format.reads(json)
  }

  override def writes(o: T): JsValue = {
    val concreteClassName = o.getClass.getName

    val format = classNameFormatMap.getOrElse(concreteClassName,
      throw new IllegalArgumentException(s"Json Formatter for a sub type '$concreteClassName' not recognized."))

    val json = format.writes(o).asInstanceOf[JsObject]
    json + (concreteClassFieldName, JsString(concreteClassName))
  }
}