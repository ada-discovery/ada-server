package org.ada.server.calc.json

import org.ada.server.field.FieldTypeHelper
import org.ada.server.models.Field
import play.api.libs.json.JsObject

import scala.reflect.runtime.universe._

trait JsonInputConverter[IN] {

  implicit val tft = FieldTypeHelper.fieldTypeFactory()

  def apply(fields: Seq[Field]): JsObject => IN

  protected[calc] def inputType: Type

  protected[calc] def specificUseClass: Option[Class[_]] = None

  protected def checkFields(fields: Seq[Field], expectedSize: Int) =
    try {
      require(fields.size == expectedSize, s"# fields is ${fields.size} but $expectedSize expected.")
    } catch {
      case e: IllegalArgumentException => throw e
    }

  protected def checkFieldsMin(fields: Seq[Field], minSize: Int) =
    try {
      require(fields.size >= minSize, s"# fields is ${fields.size} but at least $minSize expected.")
    } catch {
      case e: IllegalArgumentException => throw e
    }
}