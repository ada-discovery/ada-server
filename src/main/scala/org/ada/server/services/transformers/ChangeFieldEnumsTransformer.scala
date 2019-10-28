package org.ada.server.services.transformers

import org.ada.server.models.datatrans.ChangeFieldEnumsTransformation
import org.incal.core.util.GroupMapList3

import scala.concurrent.ExecutionContext.Implicits.global

private class ChangeFieldEnumsTransformer extends AbstractDataSetMetaTransformer[ChangeFieldEnumsTransformation] {

  override protected def execInternal(
    spec: ChangeFieldEnumsTransformation
  ) = {
    val sourceDsa = dsaSafe(spec.sourceDataSetId)
    val fieldNameEnumMap = spec.fieldNameOldNewEnums.toGroupMap
    for {
      // all the fields
      fields <- sourceDsa.fieldRepo.find()

      // new fields with replaced enum values
      newFields = fields.map(field =>
        fieldNameEnumMap.get(field.name).map { newEnums =>
          val newEnumMap = newEnums.toMap

          val newNumValues = field.enumValues.map { case (index, value) =>
            val newValue = newEnumMap.get(value).getOrElse(value)
            (index, newValue)
          }

          field.copy(enumValues = newNumValues)
        }.getOrElse(
          field
        )
      )
    } yield
      (sourceDsa, newFields, Nil)
  }
}
