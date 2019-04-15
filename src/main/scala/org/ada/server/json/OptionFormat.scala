package org.ada.server.json

import play.api.libs.json._

class OptionFormat[T](implicit val format: Format[T]) extends Format[Option[T]] {
  override def reads(json: JsValue): JsResult[Option[T]] =
    json match {
      case JsNull => JsSuccess(None)
      case _ => format.reads(json).map(Some(_))
    }

  override def writes(o: Option[T]): JsValue =
    o.map(format.writes(_)).getOrElse(JsNull)
}