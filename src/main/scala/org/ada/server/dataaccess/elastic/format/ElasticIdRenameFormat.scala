package org.ada.server.dataaccess.elastic.format

import org.ada.server.models.DataSetFormattersAndIds.JsObjectIdentity
import play.api.libs.json._
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat

import scala.collection.mutable.{Map => MMap}

object ElasticIdRenameUtil {

  val originalIdName = JsObjectIdentity.name
  val storedIdName = "__id"

  implicit val elasticIdFormat: Format[JsValue] = new FlatBSONObjectIDFormat
  def wrapFormat[T](format: Format[T]): Format[T] = new ElasticIdRenameFormat(format)

  def rename(fieldName: String) =
    if (fieldName.equals(originalIdName))
      storedIdName
    else
      fieldName

  def unrename(fieldName: String) =
    if (fieldName.equals(storedIdName))
      originalIdName
    else
      fieldName

  private class FlatBSONObjectIDFormat extends Format[JsValue] {

    override def reads(json: JsValue): JsResult[JsValue] =
      json match {
        case jsObject: JsObject =>
          (json \ storedIdName) match {

            case JsDefined(jsValue: JsValue) =>
              jsValue match {
                case jsString: JsString =>
                  BSONObjectID.parse(jsString.value).toOption.map { id =>
                    val mergedValues = MMap[String, JsValue]()
                    mergedValues.++=(jsObject.value)
                    mergedValues.-=(storedIdName)
                    mergedValues.+=((originalIdName, Json.toJson(id)))

                    JsSuccess(JsObject(mergedValues))
                  }.getOrElse(
                    JsError(s"JSON $jsValue cannot be parsed to BSON Object ID.")
                  )

                // TODO: once all the indeces are reindexed to 5.6 we can remove this part
                case jsIdObject: JsObject =>
                  jsIdObject.asOpt[BSONObjectID].map { id =>
                    val mergedValues = MMap[String, JsValue]()
                    mergedValues.++=(jsObject.value)
                    mergedValues.-=(storedIdName)
                    mergedValues.+=((originalIdName, Json.toJson(id)))

                    JsSuccess(JsObject(mergedValues))
                  }.getOrElse(
                    JsError(s"JSON $jsValue cannot be parsed to BSON Object ID.")
                  )

                case _ => JsError(s"JSON $jsValue cannot be parsed to BSON Object ID.")
              }
            case _ =>
              JsSuccess(jsObject)
          }

        case _ => JsSuccess(json) // JsError(s"JSON $json is not an object.")
      }

    override def writes(json: JsValue): JsValue =
      json match {
        case jsObject: JsObject =>
          (json \ originalIdName) match {

            case JsDefined(jsValue) =>
              jsValue.asOpt[BSONObjectID].map { id =>
                val mergedValues = MMap[String, JsValue]()
                mergedValues.++=(jsObject.value)
                mergedValues.-=(originalIdName)
                mergedValues.+=((storedIdName, JsString(id.stringify)))
                JsObject(mergedValues)
              }.getOrElse(throw new RuntimeException(s"JSON $jsValue cannot be parsed to BSON Object ID."))

            case _ =>
              json
          }

        case _ => json
      }
  }

  private class ElasticIdRenameFormat[T](format: Format[T]) extends Format[T] {

    override def reads(json: JsValue): JsResult[T] =
      format.compose(elasticIdFormat).reads(json)

    override def writes(o: T): JsValue =
      format.transform(elasticIdFormat).writes(o)
  }
}