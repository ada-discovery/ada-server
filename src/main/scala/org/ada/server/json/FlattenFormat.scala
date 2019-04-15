package org.ada.server.json

import org.ada.server.dataaccess.JsonUtil
import play.api.libs.json._

class FlattenFormat[T](
    val format: Format[T],
    delimiter: String = ".",
    excludedFieldNames: Set[String] = Set()
  ) extends Format[T] {

  override def reads(json: JsValue): JsResult[T] = {
    val newJson = json match {
      case jsObject: JsObject => JsonUtil.deflatten(jsObject, delimiter)
      case json => json
    }

    format.reads(newJson)
  }

  override def writes(json: T): JsValue =
    format.writes(json) match {
      case jsObject: JsObject => JsonUtil.flatten(jsObject, delimiter, excludedFieldNames)
      case json => json
    }
}