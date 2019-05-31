package org.ada.server.services.transformers

import org.ada.server.models.datatrans.RenameFieldsTransformation
import play.api.libs.json.{JsNull, JsObject}

import scala.concurrent.ExecutionContext.Implicits.global

class RenameFieldsTransformer extends AbstractDataSetTransformer[RenameFieldsTransformation] {

  override protected def execInternal(
    spec: RenameFieldsTransformation
  ) = {
    val sourceDsa = dsaf(spec.sourceDataSetId).get
    val oldNewFieldNameMap = spec.fieldOldNewNames.toMap

    for {
      // all the fields
      fields <- sourceDsa.fieldRepo.find()

      // new fields with replaced names
      newFields = fields.map(field =>
        field.copy(name = oldNewFieldNameMap.getOrElse(field.name, field.name))
      )

      // full data stream
      origStream <- sourceDsa.dataSetRepo.findAsStream()

      // replace field names and create a new stream
      inputStream = origStream.map{ json =>
        val replacedFieldValues = spec.fieldOldNewNames.map { case (oldFieldName, newFieldName) =>
          (newFieldName, (json \ oldFieldName).getOrElse(JsNull))
        }
        val trimmedJson = spec.fieldOldNewNames.map(_._1).foldLeft(json)(_.-(_))
        trimmedJson ++ JsObject(replacedFieldValues)
      }
    } yield
      (sourceDsa, newFields, Some(inputStream))
  }
}
