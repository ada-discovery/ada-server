package field

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.ada.server.field.FieldTypeHelper
import org.ada.server.models.{FieldTypeId, FieldTypeSpec}
import org.scalatest.{Assertion, _}
import play.api.libs.json._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

class StreamJsonFieldTypeInferrerTest extends FlatSpec with Matchers {

  private val fti = FieldTypeHelper.jsonFieldTypeInferrer

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Null field type" should "be inferred only for null values" in {
    val shouldBeNullType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Null, false))_

    for {
      _ <- shouldBeNullType(Seq())
      _ <- shouldBeNullType(Seq(JsNull))
      _ <- shouldBeNullType(Seq(JsString("")))
      _ <- shouldBeNullType(Seq(JsString(" na")))
      _ <- shouldBeNullType(Seq(JsString(""), JsString("N/A"), JsString(""), JsString(""), JsNull))
    } yield
      succeed
  }

  "Int field type" should "be inferred only for int values" in {
    val shouldBeIntType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Integer, false))_

    for {
      _ <- shouldBeIntType(Seq(JsString("128")))
      _ <- shouldBeIntType(Seq(JsNumber(128)))
      _ <- shouldBeIntType(Seq(JsString(" 8136"), JsString("na"), JsNull, JsString("12")))
      _ <- shouldBeIntType(Seq(JsString("    "), JsString(" 92")))
    } yield
      succeed
  }

  "Double field type" should "be inferred only for double values" in {
    val shouldBeDoubleType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Double, false))_

    for {
      _ <- shouldBeDoubleType (Seq(JsString("128.0")))
      _ <- shouldBeDoubleType (Seq(JsString(" 8136"), JsString("na"), JsNull, JsString("12.1")))
      _ <- shouldBeDoubleType (Seq(JsString("    "), JsString(" 92.085")))
    } yield
      succeed
  }

  "Boolean field type" should "be inferred only for boolean values" in {
    val shouldBeBooleanType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Boolean, false))_

    for {
      _ <- shouldBeBooleanType (Seq(JsString("true")))
      _ <- shouldBeBooleanType (Seq(JsString(" false"), JsString("na"), JsNull, JsString("1")))
      _ <- shouldBeBooleanType (Seq(JsString(" 0"), JsString("na"), JsString("1"), JsString("1")))
      _ <- shouldBeBooleanType (Seq(JsString("1"), JsString("0"), JsString(""), JsString(" false"), JsString("na")))
    } yield
      succeed
  }

  "Date field type" should "be inferred only for date values" in {
    val shouldBeDateType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Date, false))_

    for {
      _ <- shouldBeDateType (Seq(JsString("2016-08-10 20:45:12")))
      _ <- shouldBeDateType (Seq(JsString(" 1999-12-01 12:12"), JsString("na"), JsNull, JsString("12.12.2012")))
      _ <- shouldBeDateType (Seq(JsString("12.NOV.2012 18:19:20"), JsString("2016-08-10 20:45:12"), JsString(""), JsString(" 12.12.2012 10:43"), JsString("na")))
    } yield
      succeed
  }

  "Enum field type" should "be inferred only for enum values" in {
    def shouldBeEnumType(enumValues: Map[Int, String]) = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Enum, false, enumValues))_

    for {
      _ <- shouldBeEnumType(Map(0 -> "Male"))(
        Seq(JsString("Male"))
      )

      _ <- shouldBeEnumType(Map(0 -> "Both", 1 -> "Female", 2 -> "Male"))(
        Seq(JsString("Male"), JsString(" Male"), JsString("Female"), JsString("Male "), JsString("na"), JsNull, JsString("Both"), JsString("Female"))
      )

      _ <- shouldBeEnumType(Map(0 -> "1", 1 -> "Sun"))(
        Seq(JsString("1"), JsString(" 1"), JsString("null"), JsString("Sun "))
      )
    } yield
      succeed
  }

  "String field type" should "be inferred only for string (non-enum) values" in {
    val shouldBeStringType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.String, false))_

    shouldBeStringType(
      Seq(
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

    for {
      _ <- shouldBeJsonType  (Seq(json1))
      _ <- shouldBeJsonType  (Seq(json1, json2, JsNull))
      _ <- shouldBeJsonType  (Seq(json1, JsString(Json.stringify(json2)), JsNull))
      _ <- shouldBeJsonType  (Seq(json2, JsString("na"), JsString("null")))
    } yield
      succeed
  }

  private def shouldBeInferredType(fieldType: FieldTypeSpec)(values: Seq[JsReadable]): Future[Assertion] =
    fti(Source.fromIterator(() => values.toIterator)).map { _.spec should be (fieldType) }
}