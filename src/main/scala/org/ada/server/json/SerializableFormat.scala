package org.ada.server.json

import play.api.libs.json.{Format, JsResult, JsValue}

class SerializableFormat[E](readsX: JsValue => JsResult[E], writesX: E => JsValue) extends Format[E] with Serializable {

  override def writes(o: E) = writesX(o)

  override def reads(json: JsValue): JsResult[E] = readsX(json)
}