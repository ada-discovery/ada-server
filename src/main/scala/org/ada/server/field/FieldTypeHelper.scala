package org.ada.server.field

import org.ada.server.field.inference.FieldTypeInferrerFactory

object FieldTypeHelper {

  val nullAliases = Set("", "na", "n/a", "null")
  val dateFormats = Seq(
    "yyyy-MM-dd HH:mm:ss.SS",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd HH",
    "yyyy-MM-dd",
    "dd-MMM-yyyy HH:mm:ss",
    "dd-MMM-yyyy HH:mm",
    "dd-MMM-yyyy HH",
    "dd-MMM-yyyy",
    "dd.MMM.yyyy HH:mm:ss",
    "dd.MMM.yyyy HH:mm",
    "dd.MMM.yyyy HH",
    "dd.MMM.yyyy",
    "dd.MM.yyyy HH:mm:ss",
    "dd.MM.yyyy HH:mm",
    "dd.MM.yyyy HH",
    "dd.MM.yyyy",
    "MM.yyyy",
    "MM/dd/yyyy"
  )
  val displayDateFormat = "yyyy-MM-dd HH:mm:ss"
  val maxEnumValuesCount = 20
  val minAvgValuesPerEnum = 1.5

  val arrayDelimiter = ","

  def fieldTypeFactory(
    nullAliases: Set[String] = nullAliases,
    dateFormats: Traversable[String] = dateFormats,
    displayDateFormat: String = displayDateFormat,
    arrayDelimiter: String = arrayDelimiter,
    booleanIncludeNumbers: Boolean = true
  ) = FieldTypeFactory(nullAliases, dateFormats, displayDateFormat, arrayDelimiter, booleanIncludeNumbers)

  def fieldTypeInferrerFactory(
    ftf: FieldTypeFactory = fieldTypeFactory(),
    maxEnumValuesCount: Int = maxEnumValuesCount,
    minAvgValuesPerEnum: Double = minAvgValuesPerEnum,
    arrayDelimiter: String = arrayDelimiter
  ) = new FieldTypeInferrerFactory(ftf, maxEnumValuesCount, minAvgValuesPerEnum, arrayDelimiter)

  val fieldTypeInferrer = fieldTypeInferrerFactory().ofString
  val jsonFieldTypeInferrer = fieldTypeInferrerFactory().ofJson
}