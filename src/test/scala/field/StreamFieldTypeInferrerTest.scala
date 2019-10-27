package field

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.ada.server.field.FieldTypeHelper
import org.ada.server.models.{FieldTypeId, FieldTypeSpec}
import org.scalatest._

import scala.concurrent.Future

// Akka streams do not allow 'null' values
class StreamFieldTypeInferrerTest extends AsyncFlatSpec with Matchers {

  private val fti = FieldTypeHelper.fieldTypeInferrer

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Null field type" should "be inferred only for null values" in {
    val shouldBeNullType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Null, false))_

    for {
      _ <- shouldBeNullType(Seq())
      _ <- shouldBeNullType(Seq(""))
      _ <- shouldBeNullType(Seq(" na"))
      _ <- shouldBeNullType(Seq("", "N/A", "", ""))
    } yield
      succeed
  }

  "Int field type" should "be inferred only for int values" in {
    val shouldBeIntType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Integer, false))_

    for {
      _ <- shouldBeIntType (Seq("128"))
      _ <- shouldBeIntType (Seq(" 8136", "na", "12"))
      _ <- shouldBeIntType (Seq("    ", " 92"))
    } yield
      succeed
  }

  "Double field type" should "be inferred only for double values" in {
    val shouldBeDoubleType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Double, false))_

    for {
      _ <- shouldBeDoubleType (Seq("128.0"))
      _ <- shouldBeDoubleType (Seq(" 8136", "na", "12.1"))
      _ <- shouldBeDoubleType (Seq("    ", " 92.085"))
    } yield
      succeed
  }

  "Boolean field type" should "be inferred only for boolean values" in {
    val shouldBeBooleanType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Boolean, false))_

    for {
      _ <- shouldBeBooleanType(Seq("true"))
      _ <- shouldBeBooleanType(Seq(" false", "na", "1"))
      _ <- shouldBeBooleanType(Seq(" 0", "na", "1", "1"))
      _ <- shouldBeBooleanType(Seq("1", "0", "", " false", "na"))
    } yield
      succeed
  }

  "Date field type" should "be inferred only for date values" in {
    val shouldBeDateType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Date, false))_

    for {
      _ <- shouldBeDateType (Seq("2016-08-10 20:45:12"))
      _ <- shouldBeDateType (Seq(" 1999-12-01 12:12", "na", "12.12.2012"))
      _ <- shouldBeDateType (Seq("12.NOV.2012 18:19:20", "2016-08-10 20:45:12", "", " 12.12.2012 10:43", "na"))
    } yield
      succeed
  }

  "Enum field type" should "be inferred only for enum values" in {
    def shouldBeEnumType(enumValues: Map[Int, String]) = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Enum, false, enumValues))_

    for {
      _ <- shouldBeEnumType(Map(0 -> "Male")) (Seq("Male", " Male"))
      _ <- shouldBeEnumType(Map(0 -> "Both", 1 -> "Female", 2 -> "Male")) (Seq(" Male", "Female", "Male ", "na", "Both", "Female"))
      _ <- shouldBeEnumType(Map(0 -> "1", 1 -> "Sun")) (Seq("1", " 1", "1", "null", "Sun "))
    } yield
      succeed
  }

  "String field type" should "be inferred only for string (non-enum) values" in {
    val shouldBeStringType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.String, false))_

    shouldBeStringType  (Seq(
      "was", "ist", "das", "traurig", "was", "ist", "Strand", "Bus", "Schule", "Gymnasium", "Geld",
      "Bus", "na", "Sport", "Kultur", "Tag", "Universitat", "Tag", "Nachrichten", "Zeit", "Radio",
      "gut", "langsam", "Tabelle", "Geist", "Bahn", "super"
    ))
  }

  "Json field type" should "be inferred only for json values" in {
    val shouldBeJsonType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Json, false))_
    val json1String = "{\"name\":\"Peter\",\"affiliation\":\"LCSB\"}"
    val json2String = "{\"name\":\"John\",\"affiliation\":\"MIT\"}"

    for {
      _ <- shouldBeJsonType(Seq(json1String))
      _ <- shouldBeJsonType(Seq(json1String, json2String))
      _ <- shouldBeJsonType(Seq(json2String, "na", "null"))
    } yield
      succeed
  }

  "Null array field type" should "be inferred only for null array values" in {
    val shouldBeNullArrayType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Null, true))_

    for {
      _ <- shouldBeNullArrayType (Seq(","))
      _ <- shouldBeNullArrayType (Seq(",,,", "na"))
      _ <- shouldBeNullArrayType (Seq(",", "N/A", ",,,", ""))
    } yield
      succeed
  }

  "Int array field type" should "be inferred only for int array values" in {
    val shouldBeIntArrayType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Integer, true))_

    for {
      _ <- shouldBeIntArrayType (Seq("128,"))
      _ <- shouldBeIntArrayType (Seq("22,8136", "na", "12,"))
      _ <- shouldBeIntArrayType (Seq(",,", "2,", "92", "12,4"))
    } yield
      succeed
  }

  "Double array field type" should "be inferred only for double values" in {
    val shouldBeDoubleArrayType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Double, true))_

    for {
      _ <- shouldBeDoubleArrayType (Seq("128.0,na"))
      _ <- shouldBeDoubleArrayType (Seq("23.12,9", "null", "12.1,  802.12"))
      _ <- shouldBeDoubleArrayType (Seq(",8136", "na", "12.1"))
      _ <- shouldBeDoubleArrayType (Seq(",2,", " 92.085"))
    } yield
      succeed
  }

  "Boolean array field type" should "be inferred only for boolean array values" in {
    val shouldBeBooleanArrayType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Boolean, true))_

    for {
      _ <- shouldBeBooleanArrayType (Seq("true,"))
      _ <- shouldBeBooleanArrayType (Seq("true,  false", "na", "1,false"))
      _ <- shouldBeBooleanArrayType (Seq(",false", "na", "1"))
      _ <- shouldBeBooleanArrayType (Seq("1,0", "0", "", " false", "na"))
    } yield
      succeed
  }

  "Date array field type" should "be inferred only for date array values" in {
    val shouldBeDateArrayType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Date, true))_

    for {
      _ <- shouldBeDateArrayType (Seq("2016-08-10 20:45:12,"))
      _ <- shouldBeDateArrayType (Seq("1999-12-01 12:12, 01.09.2002 20:12", "na", ",12.12.2012"))
      _ <- shouldBeDateArrayType (Seq(", 1999-12-01 12:12", "na", "12.12.2012"))
      _ <- shouldBeDateArrayType (Seq("12.NOV.2012 18:19:20, 01.09.2002 20:12", "2016-08-10 20:45:12", "", " 12.12.2012 10:43", "na"))
    } yield
      succeed
  }

  "Enum array field type" should "be inferred only for enum array values" in {
    def shouldBeEnumArrayType(enumValues: Map[Int, String]) = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Enum, true, enumValues))_

    shouldBeEnumArrayType(Map(0 -> "Male")) (Seq(
      "Male,", "Male, Male", "Male,null", "Male,na", "na", "Male,Male,Male,na,Male", ",,,Male", "Male,Male,,,", ",,,,",
      ",null,na", "Male,,", "Male,Male,Male,,,", ",Male,Male,,,", "Male,Male,Male,,Male,Male", ",,Male,", ",,Male,Male",
      "Male,Male,Male,,Male,Male", "Male,,Male,,Male,,Male,,", "Male,na,na,na,,Male", "Male,,Male,Male,Male,na,Male",
      "Male, Male, Male", "Male,    Male", "Male,  Male", "Male, na, na, Male,     Male"
    ))
  }

//  "String array field type" should "be inferred only for string (non-enum) array values" in {
//    val shouldBeStringType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.String, true))_
//
//    shouldBeStringType  (Seq(
//      "was,was", "ist", "das", "traurig", "was", "ist", "Strand", "Bus", "Schule", "Gymnasium", "Geld",
//      "Bus", null, "na", "Sport", "Kultur", "Tag", "Universitat", "Tag", "Nachrichten", "Zeit", "Radio",
//      "gut", "langsam", "Tabelle", "Geist", "Bahn", "super"
//    ))
//  }

  "Json array field type" should "be inferred only for json array values" in {
    val shouldBeJsonType = shouldBeInferredType(FieldTypeSpec(FieldTypeId.Json, true))_
    val json1String = "{\"name\":\"Peter\",\"affiliation\":\"LCSB\"}"
    val json2String = "{\"name\":\"John\",\"affiliation\":\"MIT\"}"

    for {
      _ <- shouldBeJsonType  (Seq("[]"))
      _ <- shouldBeJsonType  (Seq(s"[$json1String]"))
      _ <- shouldBeJsonType  (Seq(s"[$json1String, null]", s"$json2String"))
      _ <- shouldBeJsonType  (Seq(s"[]", s"$json2String", "na", "null"))
    } yield
      succeed
  }

  private def shouldBeInferredType(fieldType: FieldTypeSpec)(values: Seq[String]): Future[Assertion] =
    fti(Source.fromIterator(() => values.toIterator)).map { _.spec should be (fieldType) }
}