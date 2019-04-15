package org.ada.server.json

import play.api.libs.json._

import scala.collection.mutable.{Map => MMap}

class RenameFieldFormat(
    originalFieldName: String,
    newFieldName: String
  ) extends Format[JsValue] {

  override def reads(json: JsValue): JsResult[JsValue] = {
    val newJson = rename(json, newFieldName, originalFieldName)
    JsSuccess(newJson)
  }

  override def writes(json: JsValue): JsValue =
    rename(json, originalFieldName, newFieldName)

  private def rename(json: JsValue, from: String, to: String): JsValue =
    json match {
      case jsObject: JsObject =>
        (jsObject \ from) match {
          case JsDefined(value) => {
            val newValues = MMap[String, JsValue]()
            newValues.++=(jsObject.value)
            newValues.-=(from)
            newValues.+=((to, value))
            JsObject(newValues)
          }
          case _ =>
            jsObject
        }

      case _ => json
    }
}
