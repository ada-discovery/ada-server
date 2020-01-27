package org.ada.server.field

import scala.reflect.runtime.universe._
import play.api.libs.json.{JsObject, JsValue}
import java.{util => ju}

import org.ada.server.AdaException
import org.incal.core.dataaccess.Criterion._
import org.ada.server.dataaccess.RepoTypes.FieldRepo
import org.ada.server.field.{FieldType, FieldTypeHelper}
import org.ada.server.models.DataSetFormattersAndIds.FieldIdentity
import org.incal.core.util.ReflectionUtil
import org.incal.core.util.ReflectionUtil.currentThreadClassLoader
import org.ada.server.models.FieldTypeSpec
import org.incal.core.FilterCondition
import org.incal.core.FilterCondition.{toCriteria, toCriterion}
import org.incal.core.dataaccess.Criterion
import reactivemongo.bson.BSONObjectID
import org.ada.server.models._

import scala.collection.Traversable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object FieldUtil {

  private val ftf = FieldTypeHelper.fieldTypeFactory()

  type NamedFieldType[T] = (String, FieldType[T])

  def valueConverters(
    fields: Traversable[Field]
  ): Map[String, String => Option[Any]] =
    fields.map { field =>
      val fieldType = ftf(field.fieldTypeSpec)
      val converter = { text: String => fieldType.valueStringToValue(text) }
      (field.name, converter)
    }.toMap

  def valueConverters(
    fieldRepo: FieldRepo,
    fieldNames: Traversable[String]
  ): Future[Map[String, String => Option[Any]]] =
    for {
      fields <- if (fieldNames.nonEmpty)
        fieldRepo.find(Seq(FieldIdentity.name #-> fieldNames.toSeq))
      else
        Future(Nil)
    } yield
      valueConverters(fields)

  // this is filter/criteria stuff... should be moved somewhere else
  def toDataSetCriteria(
    fieldRepo: FieldRepo,
    conditions: Seq[FilterCondition]
  ): Future[Seq[Criterion[Any]]] =
    for {
      valueConverters <- {
        val fieldNames = conditions.map(_.fieldName)
        FieldUtil.valueConverters(fieldRepo, fieldNames)
      }
    } yield
      conditions.map(toCriterion(valueConverters)).flatten

  def caseClassToFlatFieldTypes[T: TypeTag](
    delimiter: String = ".",
    excludedFieldSet: Set[String] = Set(),
    treatEnumAsString: Boolean = false
  ): Traversable[(String, FieldTypeSpec)] =
    caseClassTypeToFlatFieldTypes(typeOf[T], delimiter, excludedFieldSet, treatEnumAsString)

  def caseClassTypeToFlatFieldTypes(
    typ: Type,
    delimiter: String = ".",
    excludedFieldSet: Set[String] = Set(),
    treatEnumAsString: Boolean = false
  ): Traversable[(String, FieldTypeSpec)] = {

    // collect member names and types
    val memberNamesAndTypes = ReflectionUtil.getCaseClassMemberNamesAndTypes(typ).filter(x => !excludedFieldSet.contains(x._1))

    // create a new mirror using the current thread for reflection
    val currentMirror = ReflectionUtil.newMirror(currentThreadClassLoader)

    memberNamesAndTypes.map { case (fieldName, memberType) =>
      try {
        val fieldTypeSpec = toFieldTypeSpec(memberType, treatEnumAsString, currentMirror)
        Seq((fieldName, fieldTypeSpec))
      } catch {
        case e: AdaException => {
          val subType = unwrapIfOption(memberType)

          val subFieldNameAndTypeSpecs = caseClassTypeToFlatFieldTypes(subType, delimiter, excludedFieldSet, treatEnumAsString)
          if (subFieldNameAndTypeSpecs.isEmpty)
            throw e
          else
            subFieldNameAndTypeSpecs.map { case (subFieldName, x) => (s"$fieldName$delimiter$subFieldName", x)}
        }
      }
    }.flatten
  }

  def isNumeric(fieldType: FieldTypeId.Value): Boolean =
    fieldType == FieldTypeId.Integer ||
      fieldType == FieldTypeId.Double ||
        fieldType == FieldTypeId.Date

  def isCategorical(fieldType: FieldTypeId.Value): Boolean =
    fieldType == FieldTypeId.String ||
      fieldType == FieldTypeId.Enum ||
        fieldType == FieldTypeId.Boolean

  implicit class FieldOps(val field: Field) {
    def isNumeric = FieldUtil.isNumeric(field.fieldType)

    def isCategorical = FieldUtil.isCategorical(field.fieldType)

    def isInteger = field.fieldType == FieldTypeId.Integer

    def isDouble = field.fieldType == FieldTypeId.Double

    def isDate = field.fieldType == FieldTypeId.Date

    def isString = field.fieldType == FieldTypeId.String

    def isEnum = field.fieldType == FieldTypeId.Enum

    def isBoolean = field.fieldType == FieldTypeId.Boolean

    def isJson = field.fieldType == FieldTypeId.Json

    def isNull = field.fieldType == FieldTypeId.Null

    def toTypeAny = toType[Any]

    def toType[T] = ftf.apply(field.fieldTypeSpec).asValueOf[T]

    def toNamedTypeAny = toNamedType[Any]

    def toNamedType[T]: NamedFieldType[T] = (field.name, toType[T])
  }

  implicit class JsonFieldOps(val json: JsObject) {

    def toValue[T](
      namedFieldType: NamedFieldType[T]
    ) = namedFieldType._2.jsonToValue(json \ namedFieldType._1)

    def toValues[T](
      fieldNameTypes: Seq[NamedFieldType[T]]
    ): Seq[Option[T]] =
      fieldNameTypes.map { case (fieldName, fieldType) =>
        json.toValue(fieldName, fieldType)
      }

    def toDisplayString[T](
      namedFieldType: NamedFieldType[T]
    ) = namedFieldType._2.jsonToDisplayString(json \ namedFieldType._1)

    def toDisplayStrings[T](
      fieldNameTypes: Seq[NamedFieldType[T]]
    ) = fieldNameTypes.map { case (fieldName, fieldType) =>
      json.toDisplayString(fieldName, fieldType)
    }
  }

  private implicit class InfixOp(val typ: Type) {

    private val optionInnerType =
      if (typ <:< typeOf[Option[_]])
        Some(typ.typeArgs.head)
      else
        None

    def matches(types: Type*) =
      types.exists(typ =:= _) ||
        (optionInnerType.isDefined && types.exists(optionInnerType.get =:= _))

    def subMatches(types: Type*) =
      types.exists(typ <:< _) ||
        (optionInnerType.isDefined && types.exists(optionInnerType.get <:< _))
  }

  private def getEnumOrdinalValues(typ: Type, mirror: Mirror): Map[Int, String] = {
    val enumValueType = unwrapIfOption(typ)
    val enum = ReflectionUtil.enum(enumValueType, mirror)

    (0 until enum.maxId).map(ordinal => (ordinal, enum.apply(ordinal).toString)).toMap
  }

  private def getJavaEnumOrdinalValues[E <: Enum[E]](typ: Type, mirror: Mirror): Map[Int, String] = {
    val enumType = unwrapIfOption(typ)
    val clazz = ReflectionUtil.typeToClass(enumType, mirror).asInstanceOf[Class[E]]
    val enumValues = ReflectionUtil.javaEnumOrdinalValues(clazz)
    enumValues.map { case (ordinal, value) => (ordinal, value.toString) }
  }

  private def unwrapIfOption(typ: Type) =
    if (typ <:< typeOf[Option[_]]) typ.typeArgs.head else typ

  @throws(classOf[AdaException])
  private def toFieldTypeSpec(
    typ: Type,
    treatEnumAsString: Boolean,
    mirror: Mirror
  ): FieldTypeSpec =
    typ match {
      // double
      case t if t matches (typeOf[Double], typeOf[Float], typeOf[BigDecimal]) =>
        FieldTypeSpec(FieldTypeId.Double)

      // int
      case t if t matches (typeOf[Int], typeOf[Long], typeOf[Byte]) =>
        FieldTypeSpec(FieldTypeId.Integer)

      // boolean
      case t if t matches typeOf[Boolean] =>
        FieldTypeSpec(FieldTypeId.Boolean)

      // enum
      case t if t subMatches typeOf[Enumeration#Value] =>
        if (treatEnumAsString)
          FieldTypeSpec(FieldTypeId.String)
        else {
          // note that for Scala Enumerations we directly use ordinal values for encoding
          val enumMap = getEnumOrdinalValues(t, mirror)
          FieldTypeSpec(FieldTypeId.Enum, false, enumMap)
        }

      // Java enum
      case t if t subMatches typeOf[Enum[_]] =>
        if (treatEnumAsString)
          FieldTypeSpec(FieldTypeId.String)
        else {
          // note that for Java Enumerations we directly use ordinal values for encoding
          val enumMap = getJavaEnumOrdinalValues(t, mirror)
          FieldTypeSpec(FieldTypeId.Enum, false, enumMap)
        }

      // string
      case t if t matches typeOf[String] =>
        FieldTypeSpec(FieldTypeId.String)

      // date
      case t if t matches typeOf[ju.Date] =>
        FieldTypeSpec(FieldTypeId.Date)

      // json
      case t if t subMatches (typeOf[JsValue], typeOf[BSONObjectID]) =>
        FieldTypeSpec(FieldTypeId.Json)

      // array/seq
      case t if t subMatches (typeOf[Seq[_]], typeOf[Set[_]]) =>
        val innerType = t.typeArgs.head
        try {
          toFieldTypeSpec(innerType, treatEnumAsString, mirror).copy(isArray = true)
        } catch {
          case e: AdaException => FieldTypeSpec(FieldTypeId.Json, true)
        }

      // map
      case t if t subMatches (typeOf[Map[String, _]]) =>
        FieldTypeSpec(FieldTypeId.Json)

      // either value or seq int
      case t if t matches (typeOf[Either[Option[Int], Seq[Int]]], typeOf[Either[Option[Long], Seq[Long]]], typeOf[Either[Option[Byte], Seq[Byte]]]) =>
        FieldTypeSpec(FieldTypeId.Integer, true)

      // either value or seq double
      case t if t matches (typeOf[Either[Option[Double], Seq[Double]]], typeOf[Either[Option[Float], Seq[Float]]], typeOf[Either[Option[BigDecimal], Seq[BigDecimal]]]) =>
        FieldTypeSpec(FieldTypeId.Double, true)

      // otherwise
      case _ =>
        val typeName =
          if (typ <:< typeOf[Option[_]])
            s"Option[${typ.typeArgs.head.typeSymbol.fullName}]"
          else
            typ.typeSymbol.fullName
        throw new AdaException(s"Type ${typeName} unknown.")
    }

  def fieldTypeOrdering(
    fieldTypeId: FieldTypeId.Value
  ): Option[Ordering[Any]] = {
    def aux[T: Ordering]: Option[Ordering[Any]] =
      Some(implicitly[Ordering[T]].asInstanceOf[Ordering[Any]])

    fieldTypeId match {
      case FieldTypeId.String => aux[String]
      case FieldTypeId.Enum => aux[Int]
      case FieldTypeId.Boolean => aux[Boolean]
      case FieldTypeId.Double => aux[Double]
      case FieldTypeId.Integer => aux[Long]
      case FieldTypeId.Date => aux[ju.Date]
      case _ => None
    }
  }

  def valueOrdering(
    value: Any
  ): Option[Ordering[Any]] = {
    def aux[T: Ordering]: Option[Ordering[Any]] =
      Some(implicitly[Ordering[T]].asInstanceOf[Ordering[Any]])

    value match {
      case _: String => aux[String]
      case _: Boolean => aux[Boolean]
      case _: Double => aux[Double]
      case _: Float => aux[Float]
      case _: Long => aux[Long]
      case _: Int => aux[Int]
      case _: Short => aux[Short]
      case _: Byte => aux[Byte]
      case _: java.util.Date => aux[java.util.Date]
      case _ => None
    }
  }

  def nameOrLabel(showFieldStyle: FilterShowFieldStyle.Value)(field: Field) =
    showFieldStyle match {
      case FilterShowFieldStyle.NamesOnly => field.name
      case FilterShowFieldStyle.LabelsOnly => field.label.getOrElse("")
      case FilterShowFieldStyle.LabelsAndNamesOnlyIfLabelUndefined => field.labelOrElseName
      case FilterShowFieldStyle.NamesAndLabels => field.labelOrElseName
    }

  // checks if field specs are the same
  // TODO: an alternative function can be introduced with relaxed criteria (i.e. checks if compatible)
  def areFieldTypesEqual(field1: Field)(field2: Field): Boolean = {
    val enums1 = field1.enumValues.toSeq.sortBy(_._1)
    val enums2 = field2.enumValues.toSeq.sortBy(_._1)

    field1.fieldType == field2.fieldType &&
      field1.isArray == field2.isArray &&
      enums1.size == enums2.size &&
      enums1.zip(enums2).forall { case ((a1, b1), (a2, b2)) => a1.equals(a2) && b1.equals(b2) }
  }

  def specToField(
    name: String,
    label: Option[String],
    typeSpec: FieldTypeSpec
  ) =
    Field(
      name,
      label,
      typeSpec.fieldType,
      typeSpec.isArray,
      typeSpec.enumValues.map { case (a,b) => (a.toString, b)},
      typeSpec.displayDecimalPlaces,
      typeSpec.displayTrueValue,
      typeSpec.displayFalseValue
    )
}