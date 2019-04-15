package org.ada.server.json

import play.api.libs.json._

private class EnumFormat[E <: Enumeration](enum: E) extends Format[E#Value]{

  def reads(json: JsValue): JsResult[E#Value] = json match {
    case JsString(s) =>
      try {
        JsSuccess(enum.withName(s))
      } catch {
        case _: NoSuchElementException => JsError(s"Enumeration expected of type: '${enum.getClass}', but it does not appear to contain the value: '$s'")
      }

    case _ => JsError("String value expected")
  }

  def writes(v: E#Value): JsValue = JsString(v.toString)
}

object EnumFormat {
  implicit def apply[E <: Enumeration](enum: E): Format[E#Value] = new EnumFormat[E](enum)
}

private class OrdinalEnumFormat[E <: Enumeration](valueIdMap: Map[E#Value, Int]) extends Format[E#Value]{

  private val idValueMap = valueIdMap.map(_.swap)

  def reads(json: JsValue): JsResult[E#Value] = json match {
    case JsNumber(id) =>
      idValueMap.get(id.toInt).map(
      JsSuccess(_)
    ).getOrElse(
      JsError(s"Enumeration does not have enum value with (sorted) id $id.")
    )

    case _ => JsError("Number value expected")
  }

  def writes(v: E#Value): JsValue = JsNumber(valueIdMap.get(v).get)
}

object OrdinalEnumFormat {

  implicit def apply[E <: Enumeration](valueIdMap: Map[E#Value, Int]): Format[E#Value] =
    new OrdinalEnumFormat(valueIdMap)

  implicit def apply[E <: Enumeration](enum: E): Format[E#Value] = {
    val valueIdMap: Map[E#Value, Int] = enum.values.toSeq.sorted.zipWithIndex.toMap
    new OrdinalEnumFormat(valueIdMap)
  }
}

object OrdinalSortedEnumFormat  {

  implicit def apply[E <: Enumeration](enum: E): Format[E#Value] = {
    val valueIdMap: Map[E#Value, Int] = enum.values.toSeq.sortBy(_.toString).zipWithIndex.toMap
    OrdinalEnumFormat[E](valueIdMap)
  }
}