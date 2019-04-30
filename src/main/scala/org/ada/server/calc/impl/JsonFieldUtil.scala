package org.ada.server.calc.impl

import org.ada.server.dataaccess.AdaConversionException
import org.ada.server.field.{FieldType, FieldTypeFactory}
import org.ada.server.models.{Field, FieldTypeId}
import play.api.libs.json.{JsObject, JsReadable, Json}
import java.{util => ju}

import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}
import scala.reflect.ClassTag

object JsonFieldUtil {

  def jsonToValue[T](
    field: Field
  )(
    implicit ftf: FieldTypeFactory
  ): JsObject => Option[T] = {
    val fieldType = ftf(field.fieldTypeSpec)
    valueGet(fieldType.asValueOf[T], field.name)
  }

  def jsonToDefinedValue[T](
    field: Field
  )(
    implicit ftf: FieldTypeFactory
  ): JsObject => T = {
    val fieldType = ftf(field.fieldTypeSpec)
    valueDefinedGet(fieldType.asValueOf[T], field.name)
  }

  def jsonToDouble(
    field: Field
  )(
    implicit ftf: FieldTypeFactory
  ): JsObject => Option[Double] = doubleScalarGet(field)

  def jsonToTuple[T1, T2](
    field1: Field,
    field2: Field
  )(
    implicit ftf: FieldTypeFactory
  ): JsObject => (Option[T1], Option[T2]) = {
    val fieldType1 = ftf(field1.fieldTypeSpec)
    val value1Get = valueGet(fieldType1.asValueOf[T1], field1.name)

    val fieldType2 = ftf(field2.fieldTypeSpec)
    val value2Get = valueGet(fieldType2.asValueOf[T2], field2.name)

    { jsObject: JsObject => (value1Get(jsObject), value2Get(jsObject))}
  }

  def jsonToTuple[T1, T2, T3](
    field1: Field, field2: Field, field3: Field)(
    implicit ftf: FieldTypeFactory
  ): JsObject => (Option[T1], Option[T2], Option[T3]) = {

    def valGet[T](field: Field) = {
      val fieldType = ftf(field.fieldTypeSpec)
      valueGet(fieldType.asValueOf[T], field.name)
    }

    val value1Get = valGet[T1](field1)
    val value2Get = valGet[T2](field2)
    val value3Get = valGet[T3](field3)

    { jsObject: JsObject => (value1Get(jsObject), value2Get(jsObject), value3Get(jsObject))}
  }

  def jsonToArrayValue[T](
    field: Field)(
    implicit ftf: FieldTypeFactory
  ): JsObject => Array[Option[T]] = {
    val spec = field.fieldTypeSpec
    val fieldType = ftf(spec)

    if (spec.isArray) {
      val arrayFieldType = fieldType.asValueOf[Array[Option[T]]]
      valueGet(arrayFieldType, field.name).andThen( result =>
        result.getOrElse(Array.empty[Option[T]])
      )
    } else {
      val scalarFieldType = fieldType.asValueOf[T]
      valueGet(scalarFieldType, field.name).andThen( result =>
        Array(result)
      )
    }
  }

  def jsonToArrayDouble(
    field: Field
  )(
    implicit ftf: FieldTypeFactory
  ): JsObject => Array[Option[Double]] = doubleArrayGet(field)

  def jsonsToValues[T](
    jsons: Traversable[JsReadable],
    fieldType: FieldType[_]
  ): Traversable[Option[T]] =
    if (fieldType.spec.isArray) {
      val typedFieldType = fieldType.asValueOf[Array[Option[T]]]

      jsons.flatMap( json =>
        typedFieldType.jsonToValue(json).map(_.toSeq).getOrElse(Seq(None))
      )
    } else {
      val typedFieldType = fieldType.asValueOf[T]
      jsons.map(typedFieldType.jsonToValue)
    }

  def jsonToDoubles(
    fields: Seq[Field]
  )(
    implicit ftf: FieldTypeFactory
  ): JsObject => Seq[Option[Double]] = {
    val valueGets: Seq[JsObject => Option[Double]] = doubleGets(fields)
    jsonToSeqAux(valueGets)
  }

  def jsonToDoublesDefined(
    fields: Seq[Field]
  )(
    implicit ftf: FieldTypeFactory
  ): JsObject => Seq[Double] = {
    val valueGets: Seq[JsObject => Option[Double]] = doubleGets(fields)
    jsonToSeqDefinedAux(valueGets)
  }

  // NOTE: This function does not support array field types it just returns double scalar for each field and returns the overall result as array
  // this is essentially the same as jsonToDoubles but instead of seq, array is returned
  def jsonToArrayDoubles(
    fields: Seq[Field]
    )(
    implicit ftf: FieldTypeFactory
  ): JsObject => Array[Option[Double]] = {
    val valueGets: Seq[JsObject => Option[Double]] = doubleGets(fields)
    jsonToArrayAux(valueGets)
  }

  def jsonToArrayDoublesDefined(
    fields: Seq[Field]
    )(
    implicit ftf: FieldTypeFactory
  ): JsObject => Array[Double] = {
    val valueGets: Seq[JsObject => Option[Double]] = doubleGets(fields)
    jsonToArrayDefinedAux(valueGets)
  }

  def jsonToBooleans(
    fields: Seq[Field]
  )(
    implicit ftf: FieldTypeFactory
  ): JsObject => Seq[Option[Boolean]] = {
    val valueGets: Seq[JsObject => Option[Boolean]] = booleanGets(fields)
    jsonToSeqAux(valueGets)
  }

  def jsonToDisplayString[T](
    field: Field
  )(
    implicit ftf: FieldTypeFactory
  ): JsObject => Option[String] = {
    val fieldType = ftf(field.fieldTypeSpec)
    displayStringGet(fieldType.asValueOf[T], field.name)
  }

  def jsonToValues[T](
    fields: Seq[Field]
  )(
    implicit ftf: FieldTypeFactory
  ): JsObject => Seq[Option[T]] = {
    val valueGets = fields.map { field =>
      val fieldType = ftf(field.fieldTypeSpec)
      valueGet(fieldType.asValueOf[T], field.name)
    }
    jsonToSeqAux(valueGets)
  }

  def jsonToNumericValue[T](
    field: Field)(
    implicit ftf: FieldTypeFactory
  ) = {
    val converter = jsonToValue[T](field)

    field.fieldType match {
      case FieldTypeId.Date =>
        (jsObject: JsObject) =>
          converter(jsObject).map(
            _.asInstanceOf[java.util.Date].getTime.asInstanceOf[T]
          )

      case _ =>
        converter
    }
  }

  def jsonToArrayNumericValue[T](
    field: Field)(
    implicit ftf: FieldTypeFactory
  ) = {
    val converter = jsonToArrayValue[T](field)

    field.fieldType match {
      case FieldTypeId.Date =>
        (jsObject: JsObject) =>
          converter(jsObject).map(_.map(
            _.asInstanceOf[java.util.Date].getTime.asInstanceOf[T])
          )

      case _ =>
        converter
    }
  }

  // aux methods
  private def doubleGets(
    fields: Seq[Field])(
    implicit ftf: FieldTypeFactory
  ):  Seq[JsObject => Option[Double]] = {
    val emptyDoubleValue = {_: JsObject => None}

    fields.map { field =>
      doubleScalarGet(field, emptyDoubleValue)
    }
  }

  private def doubleScalarGet(
    field: Field,
    emptyDoubleValue: JsObject => Option[Double] = {_: JsObject => None})(
    implicit ftf: FieldTypeFactory
  ):  JsObject => Option[Double] = {
   //    val specFieldTypeMap = MMap[FieldTypeSpec, FieldType[_]]()

    val spec = field.fieldTypeSpec
    val fieldType = ftf(spec)
    //      val fieldType = specFieldTypeMap.getOrElseUpdate(spec, ftf(spec))

    // helper function to create a getter for a (scalar) value
    def scalarValueGet[T] = valueGet(fieldType.asValueOf[T], field.name)
    def arrayValueGet[T] = arrayHeadValueGet(fieldType.asValueOf[T], field.name)

    if (spec.isArray) {
      field.fieldType match {
        case FieldTypeId.Double => arrayValueGet[Double]
        case FieldTypeId.Integer => arrayValueGet[Long].andThen(_.map(_.toDouble))
        case FieldTypeId.Date => arrayValueGet[ju.Date].andThen(_.map(_.getTime.toDouble))
        case _ => emptyDoubleValue
      }
    } else {
      field.fieldType match {
        case FieldTypeId.Double => scalarValueGet[Double]
        case FieldTypeId.Integer => scalarValueGet[Long].andThen(_.map(_.toDouble))
        case FieldTypeId.Date => scalarValueGet[ju.Date].andThen(_.map(_.getTime.toDouble))
        case _ => emptyDoubleValue
      }
    }
  }

  private def doubleArrayGet(
    field: Field,
    emptyDoubleValue: JsObject => Array[Option[Double]] = {_: JsObject => Array()})(
    implicit ftf: FieldTypeFactory
  ):  JsObject => Array[Option[Double]] =
    field.fieldType match {
      case FieldTypeId.Double => jsonToArrayValue[Double](field)
      case FieldTypeId.Integer => jsonToArrayValue[Long](field).andThen(_.map(_.map(_.toDouble)))
      case FieldTypeId.Date => jsonToArrayValue[ju.Date](field).andThen(_.map(_.map(_.getTime.toDouble)))
      case _ => emptyDoubleValue
    }

  private def booleanGets(
    fields: Seq[Field])(
    implicit ftf: FieldTypeFactory
  ):  Seq[JsObject => Option[Boolean]] = {
    val emptyValue = {_: JsObject => None}

    fields.map { field =>
      booleanScalarGet(field, emptyValue)
    }
  }

  private def booleanScalarGet(
    field: Field,
    emptyBooleanValue: JsObject => Option[Boolean] = {_: JsObject => None})(
    implicit ftf: FieldTypeFactory
  ):  JsObject => Option[Boolean] = {
    val spec = field.fieldTypeSpec
    val fieldType = ftf(spec)

    if (spec.isArray) {
      field.fieldType match {
        case FieldTypeId.Boolean => arrayHeadValueGet(fieldType.asValueOf[Boolean], field.name)
        case _ => emptyBooleanValue
      }
    } else {
      field.fieldType match {
        case FieldTypeId.Boolean => valueGet(fieldType.asValueOf[Boolean], field.name)
        case _ => emptyBooleanValue
      }
    }
  }

  private def valueGet[T](fieldType: FieldType[T], fieldName: String) =
    (jsObject: JsObject) =>
      fieldType.jsonToValue(jsObject \ fieldName)

  private def valueDefinedGet[T](fieldType: FieldType[T], fieldName: String) =
    (jsObject: JsObject) =>
      fieldType.jsonToValue(jsObject \ fieldName).getOrElse(
        throw new AdaConversionException(s"All values for the field ${fieldName} are expected to be defined but got None for JSON:\n ${Json.prettyPrint(jsObject)}.")
      )

  // helper function to create a getter for array head value
  def arrayHeadValueGet[T](fieldType: FieldType[T], fieldName: String) = {
    val arrayFieldType = fieldType.asValueOf[Array[Option[T]]]

    valueGet(arrayFieldType, fieldName).andThen(
      _.flatMap(_.headOption).flatten
    )
  }

  private def displayStringGet[T](fieldType: FieldType[T], fieldName: String) = {
    jsObject: JsObject =>
      fieldType.jsonToDisplayStringOptional(jsObject \ fieldName)
  }

  private def jsonToSeqAux[T](
    valueGets: Seq[JsObject => T]
  ): JsObject => Seq[T] =
    (jsObject: JsObject) => valueGets.map(_(jsObject))

  private def jsonToArrayAux[T: ClassTag](
    valueGets: Seq[JsObject => T]
  ): JsObject => Array[T] = {
    val size = valueGets.size

    { jsObject: JsObject =>
      val result = new Array[T](size)
      for (i <- 0 to size - 1) yield {
        result.update(i, valueGets(i)(jsObject))
      }
      result
    }
  }

  private def jsonToSeqDefinedAux[T](
    valueGets: Seq[JsObject => Option[T]]
  ): JsObject => Seq[T] =
    (jsObject: JsObject) =>
      valueGets.map(_(jsObject).getOrElse(
        throw new AdaConversionException(s"All values are expected to be defined but got None for JSON:\n ${Json.prettyPrint(jsObject)}.")
      ))

  private def jsonToArrayDefinedAux[T: ClassTag](
    valueGets: Seq[JsObject => Option[T]]
  ): JsObject => Array[T] = {
    val size = valueGets.size

    { jsObject: JsObject =>
      val result = new Array[T](size)
      for (i <- 0 to size - 1) yield {
        result.update(i, valueGets(i)(jsObject).getOrElse(
          throw new AdaConversionException(s"All values are expected to be defined but got None for JSON:\n ${Json.prettyPrint(jsObject)}.")
        ))
      }
      result
    }
  }
}