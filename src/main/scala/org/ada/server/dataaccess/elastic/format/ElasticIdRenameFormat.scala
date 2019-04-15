package org.ada.server.dataaccess.elastic.format

import org.ada.server.models.DataSetFormattersAndIds.JsObjectIdentity
import org.ada.server.json.RenameFieldFormat
import play.api.libs.json._

class ElasticIdRenameFormat[T](format: Format[T]) extends  Format[T] {

  private val jsFormat = ElasticIdRenameUtil.createFormat

  override def reads(json: JsValue): JsResult[T] =
    format.compose(jsFormat).reads(json)

  override def writes(o: T): JsValue =
    format.transform(jsFormat).writes(o)
}

object ElasticIdRenameUtil {
  val originalIdName = JsObjectIdentity.name
  val newIdName = "__id"
  val newIdNameSuffix = ".$oid"
  val newIdNameWithSuffix = newIdName + newIdNameSuffix

  def createFormat: Format[JsValue] = new RenameFieldFormat(originalIdName, newIdName)

  def rename(fieldName: String, withSuffix: Boolean) =
    if (fieldName.equals(originalIdName))
      if (withSuffix)
        newIdNameWithSuffix
      else
        newIdName
    else
      fieldName

  def unrename(fieldName: String) =
    if (fieldName.equals(newIdName) || fieldName.equals(newIdNameWithSuffix))
      originalIdName
    else
      fieldName
}