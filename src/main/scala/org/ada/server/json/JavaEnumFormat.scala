package org.ada.server.json

import org.incal.core.util.ReflectionUtil
import play.api.libs.json._

import scala.reflect.ClassTag

object JavaEnumFormat  {

  private def enumReads[E <: Enum[E]](clazz: Class[E]): Reads[E] = new Reads[E] {
    def reads(json: JsValue): JsResult[E] = json match {
      case JsString(s) => {
        try {
          JsSuccess(Enum.valueOf[E](clazz, s))
        } catch {
          case _: IllegalArgumentException => JsError(s"Java enum expected of type: '${clazz.getName}', but it does not appear to contain the value: '$s'")
        }
      }
      case _ => JsError("String value expected")
    }
  }

  private def enumWrites[E <: Enum[E]]: Writes[E] = new Writes[E] {
    def writes(v: E): JsValue = JsString(v.toString)
  }

  implicit def apply[E <: Enum[E]](implicit classTag: ClassTag[E]): Format[E] = {
    Format(enumReads[E](classTag.runtimeClass.asInstanceOf[Class[E]]), enumWrites[E])
  }
}

object JavaOrdinalEnumFormat  {

  private def enumReads[E <: Enum[E]](idValueMap: Map[Int, E]
  ): Reads[E] = new Reads[E] {
    def reads(json: JsValue): JsResult[E] = json match {
      case JsNumber(id) =>
        idValueMap.get(id.toInt).map(
          JsSuccess(_)
        ).getOrElse(
          JsError(s"Java enum does not have enum value with (sorted) id $id.")
        )

      case _ => JsError("Number value expected")
    }
  }

  private def enumWrites[E <: Enum[E]](
    valueIdMap: Map[E, Int]
  ): Writes[E] = new Writes[E] {
    def writes(v: E): JsValue = JsNumber(valueIdMap.get(v).get)
  }

  implicit def apply[E <: Enum[E]](implicit classTag: ClassTag[E]): Format[E] = {
    val clazz = classTag.runtimeClass.asInstanceOf[Class[E]]
    val idValueMap = ReflectionUtil.javaEnumOrdinalValues(clazz)

    Format(enumReads[E](idValueMap), enumWrites[E](idValueMap.map(_.swap)))
  }
}