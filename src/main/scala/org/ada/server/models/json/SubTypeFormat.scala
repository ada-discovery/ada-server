package org.ada.server.models.json

import play.api.libs.json._

case class ManifestedFormat[E: Manifest](format: Format[E]){
  val man = manifest[E]
  val runtimeClass = man.runtimeClass.asInstanceOf[Class[E]]
}

class SubTypeFormat[T](formats: Traversable[ManifestedFormat[_ <: T]]) extends Format[T] {

  private val concreteClassFieldName = "concreteClass"

  val classNameFormatMap = formats.map{ classAndFormat => (
    classAndFormat.man.runtimeClass.getName,
    classAndFormat.format.asInstanceOf[Format[T]])
  }.toMap

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