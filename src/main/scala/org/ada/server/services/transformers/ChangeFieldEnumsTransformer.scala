package org.ada.server.services.transformers

import org.ada.server.models.datatrans.ChangeFieldEnumsTransformation

import scala.concurrent.ExecutionContext.Implicits.global

class ChangeFieldEnumsTransformer extends AbstractDataSetTransformer[ChangeFieldEnumsTransformation] {

  override protected def execInternal(
    spec: ChangeFieldEnumsTransformation
  ) = {
    val sourceDsa = dsaf(spec.sourceDataSetId).get
    val fieldNameEnumMap = spec.fieldNameOldNewEnums.toMap

    for {
      // all the fields
      fields <- sourceDsa.fieldRepo.find()

      // new fields with replaced enum values
      newFields = fields.map(field =>
        fieldNameEnumMap.get(field.name).map { newEnums =>
          val newEnumMap = newEnums.toMap

          val newNumValues = field.numValues.map { case (index, value) =>
            val newValue = newEnumMap.get(value).getOrElse(value)
            (index, newValue)
          }

          field.copy(numValues = newNumValues)
        }.getOrElse(
          field
        )
      )
    } yield
      (sourceDsa, newFields, None)
  }
}
