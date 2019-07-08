package org.ada.server.dataaccess.ignite

import org.ada.server.models.FieldTypeId
import FieldTypeId._
import org.apache.ignite.IgniteBinary
import org.apache.ignite.binary.{BinaryObject, BinaryType}
import org.apache.ignite.internal.binary.BinaryObjectImpl
import play.api.libs.json.Json
import java.util.Date

import org.ada.server.json.EnumFormat
import play.api.libs.json._
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat

import scala.collection.JavaConversions._

object BinaryJsonUtil {

  def escapeIgniteFieldName(fieldName : String) =
    fieldName.replaceAll("-", "\\u2014")

  def unescapeFieldName(fieldName : String) =
    fieldName.replaceAll("u2014", "-")

  def toJsObject(binaryObject: BinaryObject, fieldNames: Option[Traversable[String]] = None): JsObject = {
    val binaryType: BinaryType = binaryObject.`type`()
    val fields = fieldNames.getOrElse(binaryType.fieldNames: Iterable[String])

    JsObject(
      fields.map { fieldName =>
        val value = binaryObject.field[Any](fieldName)
        val fieldType = binaryType.fieldTypeName(fieldName)
        (unescapeFieldName(fieldName), toJson(value))
      }.toSeq
    )
  }

  def toJsObject(result: Traversable[(String, Any)]): JsObject =
    JsObject(
      result.map { case (fieldName, value) =>
        (unescapeFieldName(fieldName), toJson(value))}.toSeq
    )

  def toJson(value: Any): JsValue =
    if (value == null)
      JsNull
    else
      value match {
        case x: JsValue => x // nothing to do
        case x: String => JsString(x)
        case x: BigDecimal => JsNumber(x)
        case x: Integer => JsNumber(BigDecimal.valueOf(x.toLong))
        case x: Long => JsNumber(BigDecimal.valueOf(x))
        case x: Double => JsNumber(BigDecimal.valueOf(x))
        case x: Boolean => JsBoolean(x)
        case x: Date => Json.toJson(x)
        case x: BSONObjectID => Json.toJson(x)
        case x: Option[_] => x.map(toJson).getOrElse(JsNull)
        case x: BinaryObject => x.deserialize().asInstanceOf[JsValue]
        case x: Array[_] => JsArray(x.map(toJson))
//        case x: Seq[JsValue] => JsArray(x)
        case x: Seq[_] => JsArray(x.map(toJson))
        case _ => throw new IllegalArgumentException(s"No JSON formatter found for the class ${value.getClass.getName}.")
      }

  def toBinaryObject(
    igniteBinary: IgniteBinary,
    fieldNameClassMap: Map[String, Class[_ >: Any]],
    cacheName: String)(
    json: JsObject
  ): BinaryObject = {
    val builder = igniteBinary.builder(cacheName)
    json.fields.foreach{ case (fieldName, jsonValue) =>
      val escapedFieldName = escapeIgniteFieldName(fieldName)
      val value = getValueFromJson(jsonValue)
      if (value != null)
        builder.setField(escapedFieldName, value)
      else
        builder.setField(escapedFieldName, null, classOf[String])
    }
    builder.build
  }

  def getValueFromJson(jsValue: JsValue): Any =
    jsValue match {
      case JsNull => null
      case JsString(value) => value
      case JsNumber(value) => value
      case JsBoolean(value) => value
      case JsArray(value) => value
      case x: JsObject => x
    }
}
