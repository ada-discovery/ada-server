package org.ada.server.field

import org.ada.server.dataaccess.AdaConversionException
import org.ada.server.field.FieldTypeInferrer.INFER_FIELD_TYPE
import org.ada.server.models.{FieldTypeId, FieldTypeSpec}
import play.api.libs.json.JsReadable

import scala.collection.mutable.{Map => MMap}

trait FieldTypeInferrer[T] {
  def apply(values: Traversable[T]): FieldType[_]
}

object FieldTypeInferrer {
  type INFER_FIELD_TYPE[T] = Traversable[T] => Option[FieldType[_]]
}

case class FieldTypeInferrerFactory(
    ftf: FieldTypeFactory,
    maxEnumValuesCount: Int,
    minAvgValuesPerEnum: Double,
    arrayDelimiter: String
  ) {

  def apply: FieldTypeInferrer[String] =
    new DisplayStringFieldTypeInferrerImpl(ftf, maxEnumValuesCount, minAvgValuesPerEnum, arrayDelimiter)

  def applyJson: FieldTypeInferrer[JsReadable] =
    new DisplayJsonFieldTypeInferrerImpl(ftf, maxEnumValuesCount, minAvgValuesPerEnum, arrayDelimiter)
}

private trait EnumFieldTypeInferrer[T] {

  protected val nullAliases: Set[String]
  protected val maxEnumValuesCount: Int
  protected val minAvgValuesPerEnum: Double

  protected def toStringValueWoNull(values: Traversable[T]): Traversable[String]

  def apply: INFER_FIELD_TYPE[T] = { values =>
    try {
      val valuesWoNull = toStringValueWoNull(values)
      val countMap = MMap[String, Int]()
      valuesWoNull.foreach { value =>
        val count = countMap.getOrElse(value.trim, 0)
        countMap.update(value.trim, count + 1)
      }
      val distinctValues = countMap.keys.toSeq.sorted

      if (distinctValues.size <= maxEnumValuesCount && minAvgValuesPerEnum * distinctValues.size <= valuesWoNull.size) {
        val enumMap = distinctValues.zipWithIndex.toMap.map(_.swap)
        Some(fieldType(enumMap))
      } else
        None
    } catch {
      case e: AdaConversionException => None
    }
  }

  protected def fieldType(enumMap: Map[Int, String]): FieldType[_] =
    EnumFieldType(nullAliases, enumMap)
}

private case class StringEnumFieldTypeInferrer(
    stringFieldType: FieldType[String],
    val maxEnumValuesCount: Int,
    val minAvgValuesPerEnum: Double
  ) extends EnumFieldTypeInferrer[String] {

  override protected val nullAliases = stringFieldType.nullAliases

  override protected def toStringValueWoNull(values: Traversable[String]) =
    values.map(stringFieldType.displayStringToValue).flatten
}

private case class JsonEnumFieldTypeInferrer(
    stringFieldType: FieldType[String],
    val maxEnumValuesCount: Int,
    val minAvgValuesPerEnum: Double
  ) extends EnumFieldTypeInferrer[JsReadable] {

  override protected val nullAliases = stringFieldType.nullAliases

  override protected def toStringValueWoNull(values: Traversable[JsReadable]) =
    values.map(stringFieldType.displayJsonToValue).flatten
}

private case class StringArrayEnumFieldTypeInferrer(
    stringArrayFieldType: FieldType[Array[Option[String]]],
    val maxEnumValuesCount: Int,
    val minAvgValuesPerEnum: Double,
    delimiter: String
  ) extends EnumFieldTypeInferrer[String] {

  override protected val nullAliases = stringArrayFieldType.nullAliases

  override protected def toStringValueWoNull(strings: Traversable[String]) =
    strings.map(stringArrayFieldType.displayStringToValue).flatten.flatten.flatten

  override protected def fieldType(enumMap: Map[Int, String]) =
    ArrayFieldType(super.fieldType(enumMap), delimiter)
}

private case class JsonArrayEnumFieldTypeInferrer(
    stringArrayFieldType: FieldType[Array[Option[String]]],
    val maxEnumValuesCount: Int,
    val minAvgValuesPerEnum: Double,
    delimiter: String
  ) extends EnumFieldTypeInferrer[JsReadable] {

  override protected val nullAliases = stringArrayFieldType.nullAliases

  override protected def toStringValueWoNull(strings: Traversable[JsReadable]) =
    strings.map(stringArrayFieldType.displayJsonToValue).flatten.flatten.flatten

  override protected def fieldType(enumMap: Map[Int, String]) =
    ArrayFieldType(super.fieldType(enumMap), delimiter)
}

private abstract class FieldTypeInferrerImpl[T] extends FieldTypeInferrer[T] {

  protected val ftf: FieldTypeFactory

  protected val stringType = ftf.stringScalar
  protected val stringArrayType = ftf.stringArray

  private val staticFieldTypes = ftf.allStaticTypes

  // these are the types that are not static but don't require any special inference
  private val semiStaticFieldTypeSpecs = Seq(
    FieldTypeSpec(FieldTypeId.Double, false),
    FieldTypeSpec(FieldTypeId.Double, true),
    FieldTypeSpec(FieldTypeId.Boolean, false),
    FieldTypeSpec(FieldTypeId.Boolean, true)
  )

  private val defaultType = staticFieldTypes.find(_.spec.fieldType == FieldTypeId.String).get

  private val prioritizedFieldTypes = Seq(
    (FieldTypeId.Null, false),
    (FieldTypeId.Null, true),
    (FieldTypeId.Boolean, false),
    (FieldTypeId.Integer, false),
    (FieldTypeId.Double, false),
    (FieldTypeId.Date, false),
    (FieldTypeId.Json, false),
    (FieldTypeId.Boolean, true),
    (FieldTypeId.Integer, true),
    (FieldTypeId.Double, true),
    (FieldTypeId.Date, true),
    (FieldTypeId.Json, true),
    (FieldTypeId.Enum, false),
    (FieldTypeId.Enum, true),
    (FieldTypeId.String, false),
    (FieldTypeId.String, true)
  )

  protected def dynamicFieldInferrers: Seq[((FieldTypeId.Value, Boolean), INFER_FIELD_TYPE[T])]
  protected def isOfStaticType(fieldType: FieldType[_]): INFER_FIELD_TYPE[T]

  private val staticFieldInferrers =
    staticFieldTypes.map(fieldType =>
      ((fieldType.spec.fieldType, fieldType.spec.isArray), isOfStaticType(fieldType))
    )

  private val semiStaticFieldInferrers =
    semiStaticFieldTypeSpecs.map(fieldTypeSpec =>
      ((fieldTypeSpec.fieldType, fieldTypeSpec.isArray), isOfStaticType(ftf(fieldTypeSpec)))
    )

  private lazy val fieldTypeInferrers: Traversable[((FieldTypeId.Value, Boolean), INFER_FIELD_TYPE[T])] =
    staticFieldInferrers ++
    semiStaticFieldInferrers ++
    dynamicFieldInferrers

  private lazy val fieldTypeInferrerMap = fieldTypeInferrers.toMap

  override def apply(values: Traversable[T]): FieldType[_] = {
    val fieldType = prioritizedFieldTypes.view.map( fieldTypeSpec =>
      fieldTypeInferrerMap.get(fieldTypeSpec).map(_(values)).flatten
    ).find(_.isDefined)

    fieldType match {
      case Some(fieldType) => fieldType.get
      // this should never happen, but who knows :)
      case None => defaultType
    }
  }
}

private class DisplayStringFieldTypeInferrerImpl(
    val ftf: FieldTypeFactory,
    val maxEnumValuesCount: Int,
    val minAvgValuesPerEnum: Double,
    val arrayDelimiter: String
  ) extends FieldTypeInferrerImpl[String] {

  override protected val dynamicFieldInferrers: Seq[((FieldTypeId.Value, Boolean), INFER_FIELD_TYPE[String])] = Seq(
    (
      (FieldTypeId.Enum, false),
      StringEnumFieldTypeInferrer(stringType, maxEnumValuesCount, minAvgValuesPerEnum).apply
    ),
    (
      (FieldTypeId.Enum, true),
      StringArrayEnumFieldTypeInferrer(stringArrayType, maxEnumValuesCount, minAvgValuesPerEnum, arrayDelimiter).apply
    )
  )

  override protected def isOfStaticType(fieldType: FieldType[_]): INFER_FIELD_TYPE[String] = { texts =>
    val passed = texts.forall( text =>
      try {
        fieldType.displayStringToValue(text)
        true
      } catch {
        case e: AdaConversionException => false
      }
    )

    if (passed)
      Some(fieldType)
    else
      None
  }
}

private class DisplayJsonFieldTypeInferrerImpl(
    val ftf: FieldTypeFactory,
    val maxEnumValuesCount: Int,
    val minAvgValuesPerEnum: Double,
    val arrayDelimiter: String
  ) extends FieldTypeInferrerImpl[JsReadable] {

  override protected val dynamicFieldInferrers: Seq[((FieldTypeId.Value, Boolean), INFER_FIELD_TYPE[JsReadable])] = Seq(
    (
      (FieldTypeId.Enum, false),
      JsonEnumFieldTypeInferrer(stringType, maxEnumValuesCount, minAvgValuesPerEnum).apply
      ),
    (
      (FieldTypeId.Enum, true),
      JsonArrayEnumFieldTypeInferrer(stringArrayType, maxEnumValuesCount, minAvgValuesPerEnum, arrayDelimiter).apply
    )
  )

  override protected def isOfStaticType(fieldType: FieldType[_]): INFER_FIELD_TYPE[JsReadable] = { texts =>
    val passed = texts.forall( text =>
      try {
        fieldType.displayJsonToValue(text)
        true
      } catch {
        case e: AdaConversionException => false
      }
    )

    if (passed)
      Some(fieldType)
    else
      None
  }
}