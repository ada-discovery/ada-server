package field

import org.ada.server.field.FieldTypeHelper
import org.ada.server.models.{FieldTypeId, FieldTypeSpec}
import org.scalatest._
import play.api.libs.json._

class JsonFieldTypeInferrerTest extends FlatSpec with Matchers {

  private val fti = FieldTypeHelper.jsonFieldTypeInferrer

  "Null field type" should "be inferred only for null values" in {
    val shouldBeNullType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Null, false))_

    shouldBeNullType (Seq())
    shouldBeNullType (Seq(JsNull))
    shouldBeNullType (Seq(JsString("")))
    shouldBeNullType (Seq(JsString(" na")))
    shouldBeNullType (Seq(JsString(""), JsString("N/A"), JsString(""), JsString(""), JsNull))
  }

  "Int field type" should "be inferred only for int values" in {
    val shouldBeIntType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Integer, false))_

    shouldBeIntType (Seq(JsString("128")))
    shouldBeIntType (Seq(JsNumber(128)))
    shouldBeIntType (Seq(JsString(" 8136"), JsString("na"), JsNull, JsString("12")))
    shouldBeIntType (Seq(JsString("    "), JsString(" 92")))
  }

  "Double field type" should "be inferred only for double values" in {
    val shouldBeDoubleType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Double, false))_

    shouldBeDoubleType (Seq(JsString("128.0")))
    shouldBeDoubleType (Seq(JsString(" 8136"), JsString("na"), JsNull, JsString("12.1")))
    shouldBeDoubleType (Seq(JsString("    "), JsString(" 92.085")))
  }

  "Boolean field type" should "be inferred only for boolean values" in {
    val shouldBeBooleanType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Boolean, false))_

    shouldBeBooleanType (Seq(JsString("true")))
    shouldBeBooleanType (Seq(JsString(" false"), JsString("na"), JsNull, JsString("1")))
    shouldBeBooleanType (Seq(JsString(" 0"), JsString("na"), JsString("1"), JsString("1")))
    shouldBeBooleanType (Seq(JsString("1"), JsString("0"), JsString(""), JsString(" false"), JsString("na")))
  }

  "Date field type" should "be inferred only for date values" in {
    val shouldBeDateType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Date, false))_

    shouldBeDateType (Seq(JsString("2016-08-10 20:45:12")))
    shouldBeDateType (Seq(JsString(" 1999-12-01 12:12"), JsString("na"), JsNull, JsString("12.12.2012")))
    shouldBeDateType (Seq(JsString("12.NOV.2012 18:19:20"), JsString("2016-08-10 20:45:12"), JsString(""), JsString(" 12.12.2012 10:43"), JsString("na")))
  }

  "Enum field type" should "be inferred only for enum values" in {
    def shouldBeEnumType(enumValues: Map[Int, String]) = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Enum, false, enumValues))_

    shouldBeEnumType(Map(0 -> "Male"))
      (Seq(JsString("Male")))

    shouldBeEnumType(Map(0 -> "Both", 1 -> "Female", 2 -> "Male"))
      (Seq(JsString("Male"), JsString(" Male"), JsString("Female"), JsString("Male "), JsString("na"), JsNull, JsString("Both"), JsString("Female")))

    shouldBeEnumType(Map(0 -> "1", 1 -> "Sun"))
      (Seq(JsString("1"), JsString(" 1"), JsString("null"), JsString("Sun ")))
  }

  "String field type" should "be inferred only for string (non-enum) values" in {
    val shouldBeStringType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.String, false))_

    shouldBeStringType  (Seq(
      "was", "ist", "das", "traurig", "was", "ist", "Strand", "Bus", "Schule", "Gymnasium", "Geld",
      "Bus", null, "na", "Sport", "Kultur", "Tag", "Universitat", "Tag", "Nachrichten", "Zeit", "Radio",
      "gut", "langsam", "Tabelle", "Geist", "Bahn", "super"
    ).map( string =>
      if (string != null)
        JsString(string)
      else
        JsNull
    )
    )
  }

  "Json field type" should "be inferred only for json values" in {
    val shouldBeJsonType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Json, false))_
    val json1 = Json.obj("name" -> "Peter", "affiliation"-> "LCSB")
    val json2 = Json.obj("name" -> "John", "affiliation" -> "MIT")

    shouldBeJsonType  (Seq(json1))
    shouldBeJsonType  (Seq(json1, json2, JsNull))
    shouldBeJsonType  (Seq(json1, JsString(Json.stringify(json2)), JsNull))
    shouldBeJsonType  (Seq(json2, JsString("na"), JsString("null")))
  }

  private def shouldBeInferredType(fieldType: FieldTypeSpec)(values: Seq[JsReadable]) =
    fti.apply(values).spec should be (fieldType)
}