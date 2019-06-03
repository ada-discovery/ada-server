package org.ada.server.models

import reactivemongo.bson.BSONObjectID

case class Field(
  name: String,
  label: Option[String] = None,
  fieldType: FieldTypeId.Value = FieldTypeId.String,
  isArray: Boolean = false,
  enumValues: Map[String, String] = Map(),
  displayDecimalPlaces: Option[Int] = None,
  displayTrueValue: Option[String] = None,
  displayFalseValue: Option[String] = None,
  aliases: Seq[String] = Seq[String](),
  var categoryId: Option[BSONObjectID] = None,
  var category: Option[Category] = None
) {
  def fieldTypeSpec: FieldTypeSpec =
    FieldTypeSpec(
      fieldType,
      isArray,
      enumValues.map { case (a,b) => (a.toInt, b) },
      displayDecimalPlaces,
      displayTrueValue,
      displayFalseValue
    )

  def labelOrElseName = label.getOrElse(name)
}

object FieldTypeId extends Enumeration {
  val Null, Boolean, Double, Integer, Enum, String, Date, Json = Value
}

case class FieldTypeSpec(
  fieldType: FieldTypeId.Value,
  isArray: Boolean = false,
  enumValues: Map[Int, String] = Map(),
  displayDecimalPlaces: Option[Int] = None,
  displayTrueValue: Option[String] = None,
  displayFalseValue: Option[String] = None
)