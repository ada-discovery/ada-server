package org.ada.server.dataaccess

import org.ada.server.dataaccess.RepoTypes.FieldRepo
import org.ada.server.field.FieldUtil.caseClassToFlatFieldTypes
import org.ada.server.models.Field
import org.incal.core.util.toHumanReadableCamel

import scala.reflect.runtime.universe.TypeTag

object CaseClassFieldRepo {

  def apply[T: TypeTag](
    excludedFieldNames: Traversable[String] = Nil,
    treatEnumAsString: Boolean = false
  ): FieldRepo = {
    val excludedFieldSet = excludedFieldNames.toSet ++ Set("_id")
    val fieldTypes = caseClassToFlatFieldTypes[T]("-", excludedFieldSet, treatEnumAsString)
    val fields = fieldTypes.map { case (name, spec) =>
      val enumValues = spec.enumValues.map { case (a, b) => (a.toString, b)}
      Field(name, Some(toHumanReadableCamel(name)), spec.fieldType, spec.isArray, enumValues)
    }.toSeq

    TransientLocalFieldRepo(fields)
  }
}