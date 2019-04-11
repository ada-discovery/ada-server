import java.text.SimpleDateFormat

import dataaccess._
import models.{FieldTypeId, FieldTypeSpec}
import org.scalatest._
import play.api.libs.json._
import java.{util => ju}

import field.{FieldType, FieldTypeHelper}

class ScalarFieldTypeTest extends FlatSpec with Matchers {

  val ftf = FieldTypeHelper.fieldTypeFactory()

  "Null field type" should "should treat all non-null values with exception" in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Null, false)).asInstanceOf[FieldType[Nothing]]

    fieldType.displayStringToValue("") should be (None)
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("aa")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("aa"))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("aa")
    }

    fieldType.valueToDisplayString(None) should be ("")

    fieldType.valueToJson(None) should be (JsNull)

    fieldType.jsonToValue(JsNull) should be (None)
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("aa"))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("aa"))
    }
  }

  "Int field type" should "should accept only integers " in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Integer, false)).asInstanceOf[FieldType[Long]]

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue("43") should be (Some(43))
    fieldType.displayStringToValue("-5887") should be (Some(-5887))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("78.5")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("true")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsNumber(43)) should be (Some(43))
    fieldType.displayJsonToValue(JsNumber(-5887)) should be (Some(-5887))
    fieldType.displayJsonToValue(JsString("43")) should be (Some(43))
    fieldType.displayJsonToValue(JsString("-5887")) should be (Some(-5887))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsNumber(78.5))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("78.5"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson("43") should be (JsNumber(43))
    fieldType.displayStringToJson("-5887") should be (JsNumber(-5887))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("78.5")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("true")
    }

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(43l)) should be ("43")
    fieldType.valueToDisplayString(Some(-5887l)) should be ("-5887")

    fieldType.valueToJson(None) should be (JsNull)
    fieldType.valueToJson(Some(43l)) should be (JsNumber(43))
    fieldType.valueToJson(Some(-5887l)) should be (JsNumber(-5887))

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsNumber(43)) should be (Some(43))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("43"))
    }
    fieldType.jsonToValue(JsNumber(-5887)) should be (Some(-5887))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("-5887"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(78.5))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(true))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(JsNumber(43)) should be ("43")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("43"))
    }
    fieldType.jsonToDisplayString(JsNumber(-5887)) should be ("-5887")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("-5887"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(78.5))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(true))
    }
  }

  "Double field type" should "should accept only doubles " in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Double, false)).asInstanceOf[FieldType[Double]]

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue("43") should be (Some(43))
    fieldType.displayStringToValue("-5887") should be (Some(-5887))
    fieldType.displayStringToValue("78.5") should be (Some(78.5))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("true")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsNumber(43)) should be (Some(43))
    fieldType.displayJsonToValue(JsNumber(-5887)) should be (Some(-5887))
    fieldType.displayJsonToValue(JsString("43")) should be (Some(43))
    fieldType.displayJsonToValue(JsString("-5887")) should be (Some(-5887))
    fieldType.displayJsonToValue(JsNumber(78.5)) should be (Some(78.5))
    fieldType.displayJsonToValue(JsString("78.5")) should be (Some(78.5))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson("43") should be (JsNumber(43))
    fieldType.displayStringToJson("-5887") should be (JsNumber(-5887))
    fieldType.displayStringToJson("78.5") should be (JsNumber(78.5))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("true")
    }

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(43)) should be ("43.0")
    fieldType.valueToDisplayString(Some(-5887)) should be ("-5887.0")
    fieldType.valueToDisplayString(Some(78.5)) should be ("78.5")

    fieldType.valueToJson(None) should be (JsNull)
    fieldType.valueToJson(Some(43)) should be (JsNumber(43))
    fieldType.valueToJson(Some(-5887)) should be (JsNumber(-5887))
    fieldType.valueToJson(Some(78.5)) should be (JsNumber(78.5))

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsNumber(43)) should be (Some(43))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("43"))
    }
    fieldType.jsonToValue(JsNumber(-5887)) should be (Some(-5887))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("-5887"))
    }
    fieldType.jsonToValue(JsNumber(78.5)) should be (Some(78.5))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(true))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(JsNumber(43)) should be ("43.0")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("43"))
    }
    fieldType.jsonToDisplayString(JsNumber(-5887)) should be ("-5887.0")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("-5887"))
    }
    fieldType.jsonToDisplayString(JsNumber(78.5)) should be ("78.5")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(true))
    }
  }

  "Boolean field type" should "should accept only booleans " in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Boolean, false)).asInstanceOf[FieldType[Boolean]]

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue("true") should be (Some(true))
    fieldType.displayStringToValue("false") should be (Some(false))
    fieldType.displayStringToValue("1") should be (Some(true))
    fieldType.displayStringToValue("0") should be (Some(false))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("54")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsBoolean(true)) should be (Some(true))
    fieldType.displayJsonToValue(JsBoolean(false)) should be (Some(false))
    fieldType.displayJsonToValue(JsString("true")) should be (Some(true))
    fieldType.displayJsonToValue(JsString("false")) should be (Some(false))
    fieldType.displayJsonToValue(JsString("1")) should be (Some(true))
    fieldType.displayJsonToValue(JsString("0")) should be (Some(false))
    fieldType.displayJsonToValue(JsNumber(1)) should be (Some(true))
    fieldType.displayJsonToValue(JsNumber(0)) should be (Some(false))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsNumber(54))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson("true") should be (JsBoolean(true))
    fieldType.displayStringToJson("false") should be (JsBoolean(false))
    fieldType.displayStringToJson("1") should be (JsBoolean(true))
    fieldType.displayStringToJson("0") should be (JsBoolean(false))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("54")
    }

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(true)) should be ("true")
    fieldType.valueToDisplayString(Some(false)) should be ("false")

    fieldType.valueToJson(None) should be (JsNull)
    fieldType.valueToJson(Some(true)) should be (JsBoolean(true))
    fieldType.valueToJson(Some(false)) should be (JsBoolean(false))

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsBoolean(true)) should be (Some(true))
    fieldType.jsonToValue(JsBoolean(false)) should be (Some(false))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("true"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("false"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(1))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(0))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(54))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(JsBoolean(true)) should be ("true")
    fieldType.jsonToDisplayString(JsBoolean(false)) should be ("false")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("true"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("false"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(1))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(0))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(54))
    }
  }

  "Date field type" should "should accept only dates " in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Date, false)).asInstanceOf[FieldType[ju.Date]]
    def toDate(text: String): ju.Date =
      new SimpleDateFormat(FieldTypeHelper.displayDateFormat).parse(text)

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue("2016-08-08 20:11:55") should be (Some(toDate("2016-08-08 20:11:55")))
    fieldType.displayStringToValue("08.08.2016") should be (Some(toDate("2016-08-08 00:00:00")))
    fieldType.displayStringToValue("1251237600000") should be (Some(new java.util.Date(1251237600000l)))
    fieldType.displayStringToValue(toDate("2016-08-08 20:11:55").getTime.toString) should be (Some(toDate("2016-08-08 20:11:55")))

    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("581251237600000")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsString("01.04.2002 18:51:20")) should be (Some(toDate("2002-04-01 18:51:20")))
    fieldType.displayJsonToValue(JsNumber(toDate("2005-04-08 20:21:11").getTime)) should be (Some(toDate("2005-04-08 20:21:11")))
    fieldType.displayJsonToValue(JsNumber(1251237600000l)) should be (Some(new java.util.Date(1251237600000l)))

    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsNumber(581251237600000l))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson("2016-08-08 20:11:55") should be (JsNumber(toDate("2016-08-08 20:11:55").getTime))
    fieldType.displayStringToJson("08.08.2016") should be (JsNumber(toDate("2016-08-08 00:00:00").getTime))
    fieldType.displayStringToJson("1251237600000") should be (JsNumber(1251237600000l))
    fieldType.displayStringToJson(toDate("2016-08-08 20:11:55").getTime.toString) should be (JsNumber(toDate("2016-08-08 20:11:55").getTime))

    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("581251237600000")
    }

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(toDate("2016-08-14 18:11:55"))) should be ("2016-08-14 18:11:55")
    fieldType.valueToDisplayString(Some(toDate("2016-05-05 00:00:00"))) should be ("2016-05-05 00:00:00")

    fieldType.valueToJson(None) should be (JsNull)
    fieldType.valueToJson(Some(toDate("2016-08-14 18:11:55"))) should be (JsNumber(toDate("2016-08-14 18:11:55").getTime))
    fieldType.valueToJson(Some(toDate("2016-05-05 00:00:00"))) should be (JsNumber(toDate("2016-05-05 00:00:00").getTime))

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsNumber(toDate("2016-08-08 20:11:55").getTime)) should be (Some(toDate("2016-08-08 20:11:55")))
    fieldType.jsonToValue(JsNumber(toDate("2016-08-08 00:00:00").getTime)) should be (Some(toDate("2016-08-08 00:00:00")))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("2016-08-08 11:12:13"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(false))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("aa"))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(JsNumber(toDate("2016-08-08 20:11:55").getTime)) should be ("2016-08-08 20:11:55")
    fieldType.jsonToDisplayString(JsNumber(toDate("2016-08-08 00:00:00").getTime)) should be ("2016-08-08 00:00:00")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("2016-08-08 11:12:13"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(false))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("aa"))
    }
  }

  "Enum field type" should "should accept only enums " in {
    val enumValues = Map(0 -> "Male", 1 -> "Female", 2 -> "Both", 3 -> "None")
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Enum, false, Some(enumValues))).asInstanceOf[FieldType[Int]]

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue(" Male") should be (Some(0))
    fieldType.displayStringToValue("Both") should be (Some(2))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("true")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("1")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsString("Male")) should be (Some(0))
    fieldType.displayJsonToValue(JsString("Both")) should be (Some(2))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("true"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("1"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson(" Male") should be (JsNumber(0))
    fieldType.displayStringToJson("Both") should be (JsNumber(2))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("true")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("1")
    }

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(0)) should be ("Male")
    fieldType.valueToDisplayString(Some(1)) should be ("Female")
    fieldType.valueToDisplayString(Some(2)) should be ("Both")
    fieldType.valueToDisplayString(Some(3)) should be ("None")
    a [AdaConversionException] should be thrownBy {
      fieldType.valueToDisplayString(Some(4))
    }

    fieldType valueToJson None should be (JsNull)
    fieldType valueToJson Some(0) should be (JsNumber(0))
    fieldType valueToJson Some(1) should be (JsNumber(1))
    fieldType valueToJson Some(2) should be (JsNumber(2))
    fieldType valueToJson Some(3) should be (JsNumber(3))
    a [AdaConversionException] should be thrownBy {
      fieldType valueToJson Some(4)
    }

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsNumber(0)) should be (Some(0))
    fieldType.jsonToValue(JsNumber(1)) should be (Some(1))
    fieldType.jsonToValue(JsNumber(2)) should be (Some(2))
    fieldType.jsonToValue(JsNumber(3)) should be (Some(3))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(4))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("43"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(true))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(JsNumber(0)) should be ("Male")
    fieldType.jsonToDisplayString(JsNumber(1)) should be ("Female")
    fieldType.jsonToDisplayString(JsNumber(2)) should be ("Both")
    fieldType.jsonToDisplayString(JsNumber(3)) should be ("None")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(4))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("43"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(true))
    }
  }

  "String field type" should "should accept only strings " in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.String, false)).asInstanceOf[FieldType[String]]

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue(null) should be (None)
    fieldType.displayStringToValue(" Male") should be (Some("Male"))
    fieldType.displayStringToValue("Lala rerwepn. fdso") should be (Some("Lala rerwepn. fdso"))
    fieldType.displayStringToValue("123") should be (Some("123"))
    fieldType.displayStringToValue("true") should be (Some("true"))

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsString(" Male")) should be (Some("Male"))
    fieldType.displayJsonToValue(JsString("Lala rerwepn. fdso")) should be (Some("Lala rerwepn. fdso"))
    fieldType.displayJsonToValue(JsString("123")) should be (Some("123"))
    fieldType.displayJsonToValue(JsString("true")) should be (Some("true"))
    fieldType.displayJsonToValue(JsNumber(1)) should be (Some("1"))
    fieldType.displayJsonToValue(JsBoolean(true)) should be (Some("true"))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson(" Male") should be (JsString("Male"))
    fieldType.displayStringToJson("Lala rerwepn. fdso") should be (JsString("Lala rerwepn. fdso"))
    fieldType.displayStringToJson("123") should be (JsString("123"))
    fieldType.displayStringToJson("true") should be (JsString("true"))

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(" Male")) should be (" Male")
    fieldType.valueToDisplayString(Some("Lala rerwepn. fdso")) should be ("Lala rerwepn. fdso")
    fieldType.valueToDisplayString(Some("123")) should be ("123")
    fieldType.valueToDisplayString(Some("true")) should be ("true")

    fieldType.valueToJson(None) should be (JsNull)
    fieldType.valueToJson(Some("Male")) should be (JsString("Male"))
    fieldType.valueToJson(Some("Lala rerwepn. fdso")) should be (JsString("Lala rerwepn. fdso"))
    fieldType.valueToJson(Some("123")) should be (JsString("123"))
    fieldType.valueToJson(Some("true")) should be (JsString("true"))

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsString("asdsagie")) should be (Some("asdsagie"))
    fieldType.jsonToValue(JsString("Lala rerwepn. fdso")) should be (Some("Lala rerwepn. fdso"))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(4))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(JsString("asdsagie")) should be ("asdsagie")
    fieldType.jsonToDisplayString(JsString("Lala rerwepn. fdso")) should be ("Lala rerwepn. fdso")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(4))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }
  }

  "JSON field type" should "should accept only JSONS" in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Json, false)).asInstanceOf[FieldType[JsObject]]

    val json1 = Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB"))
    val json1String = "{\"name\":\"Peter\",\"affiliation\":\"LCSB\"}"

    val json2 = Json.obj("name" -> JsString("John"), "affiliation" -> JsString("MIT"))
    val json2String = "{\"name\":\"John\",\"affiliation\":\"MIT\"}"

    val jsonArray = JsArray(Seq(json1, json2))
    val jsonArrayString = s"[$json1,$json2]"

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue(null) should be (None)
    fieldType.displayStringToValue(json1String) should be (Some(json1))
    fieldType.displayStringToValue(json2String) should be (Some(json2))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue(jsonArrayString)
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("Lala")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("123")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("true")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(json1) should be (Some(json1))
    fieldType.displayJsonToValue(json2) should be (Some(json2))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(jsonArray)
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("Lala"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsNumber(123))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsBoolean(true))
    }


    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson(json1String) should be (json1)
    fieldType.displayStringToJson(json2String) should be (json2)
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson(jsonArrayString)
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("Lala")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("123")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("true")
    }

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(json1)) should be (json1String)
    fieldType.valueToDisplayString(Some(json2)) should be (json2String)

    fieldType.valueToJson(None) should be (JsNull)
    fieldType.valueToJson(Some(json1)) should be (json1)
    fieldType.valueToJson(Some(json2)) should be (json2)

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(json1) should be (Some(json1))
    fieldType.jsonToValue(json2) should be (Some(json2))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(jsonArray)
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("Lala"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(4))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(true))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(json1) should be (json1String)
    fieldType.jsonToDisplayString(json2) should be (json2String)
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(jsonArray)
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("Lala"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(4))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(true))
    }
  }
}