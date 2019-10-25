package field

import java.text.SimpleDateFormat
import java.{util => ju}

import org.ada.server.dataaccess.AdaConversionException
import org.ada.server.field.{FieldType, FieldTypeHelper}
import org.ada.server.models.{FieldTypeId, FieldTypeSpec}
import org.scalatest._
import play.api.libs.json._

class ArrayFieldTypeTest extends FlatSpec with Matchers {

  val ftf = FieldTypeHelper.fieldTypeFactory()

  "Null array field type" should "treat all non-null values with exception" in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Null, true)).asInstanceOf[FieldType[Array[Option[Nothing]]]]

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue(",,,").get should be (Array(None, None, None, None))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("aa,b")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsArray()).get should be (Array())
    fieldType.displayJsonToValue(JsArray(Seq(JsNull,JsNull,JsNull))).get should be (Array(None, None, None))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsArray(Seq(JsString("aa"))))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson(",,,") should be (JsArray(Seq(JsNull, JsNull, JsNull, JsNull)))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("aa,b")
    }

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(Array())) should be ("")
    fieldType.valueToDisplayString(Some(Array(None, None, None))) should be (",,")

    fieldType.valueToJson(None) should be (JsNull)
    fieldType.valueToJson(Some(Array())) should be (JsArray())
    fieldType.valueToJson(Some(Array(None, None, None))) should be (JsArray(Seq(JsNull, JsNull, JsNull)))

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsArray()).get should be (Array())
    fieldType.jsonToValue(JsArray(Seq(JsNull, JsNull))).get should be (Array(None, None))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsArray(Seq(JsString("aa"))))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(JsArray()) should be ("")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNull, JsNull))) should be (",")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsArray(Seq(JsString("aa"))))
    }
  }

  "Int array field type" should "accept only integers " in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Integer, true)).asInstanceOf[FieldType[Array[Option[Long]]]]

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue(",,,").get should be (Array(None, None, None, None))
    fieldType.displayStringToValue("43").get should be (Array(Some(43)))
    fieldType.displayStringToValue("-5887").get should be (Array(Some(-5887)))
    fieldType.displayStringToValue("43, -876,  2").get should be (Array(Some(43), Some(-876), Some(2)))
    fieldType.displayStringToValue("3,, 21").get should be (Array(Some(3), None, Some(21)))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("78.5")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("true")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("43,a")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsArray()).get should be (Array())
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(43)))).get should be (Array(Some(43)))
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(-5887)))).get should be (Array(Some(-5887)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("43")))).get should be (Array(Some(43)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("-5887")))).get should be (Array(Some(-5887)))
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(43), JsNumber(-876), JsNumber(2)))).get should be (Array(Some(43), Some(-876), Some(2)))
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(3),JsNull,JsNumber(21)))).get should be (Array(Some(3), None, Some(21)))
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(3),JsString("-8535"),JsNull,JsString("22411")))).get should be (Array(Some(3), Some(-8535), None, Some(22411)))
    // TODO: Is the handling of JsString correct?
    fieldType.displayJsonToValue(JsString("78")).get should be (Array(Some(78)))
    fieldType.displayJsonToValue(JsString("78,,21")).get should be (Array(Some(78),None,Some(21)))

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
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsArray(Seq(JsNumber(43),JsString("a"))))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson("43") should be (JsArray(Seq(JsNumber(43))))
    fieldType.displayStringToJson("-5887") should be (JsArray(Seq(JsNumber(-5887))))
    fieldType.displayStringToJson("43, -876,  2") should be (JsArray(Seq(JsNumber(43), JsNumber(-876), JsNumber(2))))
    fieldType.displayStringToJson("3,, 21") should be (JsArray(Seq(JsNumber(3), JsNull, JsNumber(21))))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("78.5")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("true")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("43,a")
    }

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(Array(Some(43l)))) should be ("43")
    fieldType.valueToDisplayString(Some(Array(Some(-5887l)))) should be ("-5887")
    fieldType.valueToDisplayString(Some(Array(Some(43), Some(-876), Some(2))))  should be ("43,-876,2")
    fieldType.valueToDisplayString(Some(Array(Some(3), None, Some(21))))  should be ("3,,21")
    fieldType.valueToDisplayString(Some(Array(Some(3), Some(-8535), None, Some(22411))))  should be ("3,-8535,,22411")

    fieldType.valueToJson(None) should be (JsNull)
    fieldType.valueToJson(Some(Array(Some(43l)))) should be (JsArray(Seq(JsNumber(43))))
    fieldType.valueToJson(Some(Array(Some(-5887l)))) should be (JsArray(Seq(JsNumber(-5887))))
    fieldType.valueToJson(Some(Array(Some(43), Some(-876), Some(2))))  should be (JsArray(Seq(JsNumber(43), JsNumber(-876), JsNumber(2))))
    fieldType.valueToJson(Some(Array(Some(3), None, Some(21))))  should be (JsArray(Seq(JsNumber(3), JsNull, JsNumber(21))))
    fieldType.valueToJson(Some(Array(Some(3), Some(-8535), None, Some(22411))))  should be (JsArray(Seq(JsNumber(3), JsNumber(-8535), JsNull, JsNumber(22411))))

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsArray(Seq(JsNumber(43)))).get should be (Array(Some(43)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(-5887)))).get should be (Array(Some(-5887)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(43)))).get should be (Array(Some(43)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(-5887)))).get should be (Array(Some(-5887)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(43), JsNumber(-876), JsNumber(2)))).get should be (Array(Some(43),Some(-876),Some(2)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(3),JsNull,JsNumber(21)))).get should be (Array(Some(3),None,Some(21)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(3),JsNumber(-8535),JsNull,JsNumber(22411)))).get should be (Array(Some(3),Some(-8535),None,Some(22411)))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(43))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("43"))
    }
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
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsArray(Seq(JsString("43"))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsArray(Seq(JsNumber(43),JsString("a"))))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(43)))) should be ("43")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(-5887)))) should be ("-5887")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(43), JsNumber(-876), JsNumber(2)))) should be ("43,-876,2")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(3),JsNull,JsNumber(21)))) should be ("3,,21")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(3),JsNumber(-8535),JsNull,JsNumber(22411)))) should be ("3,-8535,,22411")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(43))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(78.5))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("78.5"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsArray(Seq(JsString("43"))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(43),JsString("a"))))
    }
  }

  "Double array field type" should "accept only doubles " in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Double, true)).asInstanceOf[FieldType[Array[Option[Double]]]]

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue(",,,").get should be (Array(None, None, None, None))
    fieldType.displayStringToValue("43").get should be (Array(Some(43)))
    fieldType.displayStringToValue("-5887").get should be (Array(Some(-5887)))
    fieldType.displayStringToValue("78.5").get should be (Array(Some(78.5)))
    fieldType.displayStringToValue("43, -876.241,  2").get should be (Array(Some(43), Some(-876.241), Some(2)))
    fieldType.displayStringToValue("3.121,, 21").get should be (Array(Some(3.121), None, Some(21)))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("true")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("43,a")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsArray()).get should be (Array())
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(43)))).get should be (Array(Some(43)))
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(78.5)))).get should be (Array(Some(78.5)))
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(-5887.2)))).get should be (Array(Some(-5887.2)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("43")))).get should be (Array(Some(43)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("78.5")))).get should be (Array(Some(78.5)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("-5887.2")))).get should be (Array(Some(-5887.2)))
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(43), JsNumber(-876), JsNumber(2)))).get should be (Array(Some(43), Some(-876), Some(2)))
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(3), JsNull, JsNumber(21)))).get should be (Array(Some(3), None, Some(21)))
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(3), JsString("-8535"), JsNull, JsString("22411")))).get should be (Array(Some(3), Some(-8535), None, Some(22411)))
    // TODO: Is the handling of JsString correct?
    fieldType.displayJsonToValue(JsString("78.5")).get should be (Array(Some(78.5)))
    fieldType.displayJsonToValue(JsString("78.5,,21")).get should be (Array(Some(78.5),None,Some(21)))

    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsNumber(78.5))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsArray(Seq(JsNumber(43),JsString("a"))))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson(",,,") should be (JsArray(Seq(JsNull, JsNull, JsNull, JsNull)))
    fieldType.displayStringToJson("43") should be (JsArray(Seq(JsNumber(43))))
    fieldType.displayStringToJson("-5887") should be (JsArray(Seq(JsNumber(-5887))))
    fieldType.displayStringToJson("78.5") should be (JsArray(Seq(JsNumber(78.5))))
    fieldType.displayStringToJson("43, -876.241,  2") should be (JsArray(Seq(JsNumber(43), JsNumber(-876.241), JsNumber(2))))
    fieldType.displayStringToJson("3.121,, 21") should be (JsArray(Seq(JsNumber(3.121), JsNull, JsNumber(21))))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("true")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("43,a")
    }

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(Array())) should be ("")
    fieldType.valueToDisplayString(Some(Array(Some(43)))) should be ("43.0")
    fieldType.valueToDisplayString(Some(Array(Some(78.5)))) should be ("78.5")
    fieldType.valueToDisplayString(Some(Array(Some(-5887.2)))) should be ("-5887.2")
    fieldType.valueToDisplayString(Some(Array(Some(43)))) should be ("43.0")
    fieldType.valueToDisplayString(Some(Array(Some(-5887.2)))) should be ("-5887.2")
    fieldType.valueToDisplayString(Some(Array(Some(43), Some(-876), Some(2)))) should be ("43.0,-876.0,2.0")
    fieldType.valueToDisplayString(Some(Array(Some(3), None, Some(21)))) should be ("3.0,,21.0")
    fieldType.valueToDisplayString(Some(Array(Some(3), Some(-8535), None, Some(22411)))) should be ("3.0,-8535.0,,22411.0")

    fieldType.valueToJson(None) should be (JsNull)
    fieldType.valueToJson(Some(Array())) should be (JsArray())
    fieldType.valueToJson(Some(Array(Some(43)))) should be (JsArray(Seq(JsNumber(43))))
    fieldType.valueToJson(Some(Array(Some(78.5)))) should be (JsArray(Seq(JsNumber(78.5))))
    fieldType.valueToJson(Some(Array(Some(-5887.2)))) should be (JsArray(Seq(JsNumber(-5887.2))))
    fieldType.valueToJson(Some(Array(Some(43)))) should be (JsArray(Seq(JsNumber(43))))
    fieldType.valueToJson(Some(Array(Some(-5887.2)))) should be (JsArray(Seq(JsNumber(-5887.2))))
    fieldType.valueToJson(Some(Array(Some(43), Some(-876), Some(2)))) should be (JsArray(Seq(JsNumber(43),JsNumber(-876),JsNumber(2))))
    fieldType.valueToJson(Some(Array(Some(3), None, Some(21)))) should be (JsArray(Seq(JsNumber(3),JsNull,JsNumber(21))))
    fieldType.valueToJson(Some(Array(Some(3), Some(-8535), None, Some(22411)))) should be (JsArray(Seq(JsNumber(3),JsNumber(-8535),JsNull,JsNumber(22411))))

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsArray()).get should be (Array())
    fieldType.jsonToValue(JsArray(Seq(JsNumber(43)))).get should be (Array(Some(43)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(78.5)))).get should be (Array(Some(78.5)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(-5887.2)))).get should be (Array(Some(-5887.2)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(43), JsNumber(-876), JsNumber(2)))).get should be (Array(Some(43), Some(-876), Some(2)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(3), JsNull, JsNumber(21)))).get should be (Array(Some(3), None, Some(21)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(3), JsNumber(-8535), JsNull, JsNumber(22411)))).get should be (Array(Some(3), Some(-8535), None, Some(22411)))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(78.5))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("78.5"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsArray(Seq(JsNumber(3), JsString("-8535"), JsNull, JsString("22411"))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsArray(Seq(JsString("43"))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsArray(Seq(JsString("78.5"))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsArray(Seq(JsString("-5887.2"))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsArray(Seq(JsNumber(43),JsString("a"))))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(JsArray()) should be ("")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(43)))) should be ("43.0")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(78.5)))) should be ("78.5")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(-5887.2)))) should be ("-5887.2")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(43), JsNumber(-876), JsNumber(2)))) should be ("43.0,-876.0,2.0")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(3), JsNull, JsNumber(21)))) should be ("3.0,,21.0")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(3), JsNumber(-8535), JsNull, JsNumber(22411)))) should be ("3.0,-8535.0,,22411.0")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(78.5))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("78.5"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(3), JsString("-8535"), JsNull, JsString("22411"))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsArray(Seq(JsString("43"))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsArray(Seq(JsString("78.5"))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsArray(Seq(JsString("-5887.2"))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(43),JsString("a"))))
    }

  }

  "Boolean array field type" should "accept only booleans " in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Boolean, true)).asInstanceOf[FieldType[Array[Option[Boolean]]]]

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue("true").get should be (Array(Some(true)))
    fieldType.displayStringToValue("false").get should be (Array(Some(false)))
    fieldType.displayStringToValue("1").get should be (Array(Some(true)))
    fieldType.displayStringToValue("0").get should be (Array(Some(false)))
    fieldType.displayStringToValue("true, false, true").get should be (Array(Some(true), Some(false), Some(true)))
    fieldType.displayStringToValue("true,,1, false").get should be (Array(Some(true), None, Some(true), Some(false)))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("54")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("true,aa")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsArray(Seq(JsBoolean(true)))).get should be (Array(Some(true)))
    fieldType.displayJsonToValue(JsArray(Seq(JsBoolean(false)))).get should be (Array(Some(false)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("true")))).get should be (Array(Some(true)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("false")))).get should be (Array(Some(false)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("1")))).get should be (Array(Some(true)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("0")))).get should be (Array(Some(false)))
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(1)))).get should be (Array(Some(true)))
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(0)))).get should be (Array(Some(false)))
    fieldType.displayJsonToValue(JsArray(Seq(JsBoolean(true), JsBoolean(false), JsBoolean(true)))).get should be (Array(Some(true), Some(false), Some(true)))
    fieldType.displayJsonToValue(JsArray(Seq(JsBoolean(true), JsNull, JsNumber(1), JsString("false")))).get should be (Array(Some(true), None, Some(true), Some(false)))
    // TODO: Is the handling of JsString correct?
    fieldType.displayJsonToValue(JsString("true")).get should be (Array(Some(true)))
    fieldType.displayJsonToValue(JsString("true,,0")).get should be (Array(Some(true),None,Some(false)))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsNumber(54))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsArray(Seq(JsBoolean(true), JsString("aa"))))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson("true") should be (JsArray(Seq(JsBoolean(true))))
    fieldType.displayStringToJson("false") should be (JsArray(Seq(JsBoolean(false))))
    fieldType.displayStringToJson("1") should be (JsArray(Seq(JsBoolean(true))))
    fieldType.displayStringToJson("0") should be (JsArray(Seq(JsBoolean(false))))
    fieldType.displayStringToJson("true, false, true") should be (JsArray(Seq(JsBoolean(true), JsBoolean(false), JsBoolean(true))))
    fieldType.displayStringToJson("true,,1, false") should be (JsArray(Seq(JsBoolean(true), JsNull, JsBoolean(true), JsBoolean(false))))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("54")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("true,aa")
    }

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(Array())) should be ("")
    fieldType.valueToDisplayString(Some(Array(Some(true)))) should be ("true")
    fieldType.valueToDisplayString(Some(Array(Some(true), Some(false), Some(true)))) should be ("true,false,true")
    fieldType.valueToDisplayString(Some(Array(Some(true), None, Some(true), Some(false)))) should be ("true,,true,false")

    fieldType.valueToJson(None) should be (JsNull)
    fieldType.valueToJson(Some(Array())) should be (JsArray())
    fieldType.valueToJson(Some(Array(Some(true)))) should be (JsArray(Seq(JsBoolean(true))))
    fieldType.valueToJson(Some(Array(Some(true), Some(false), Some(true)))) should be (JsArray(Seq(JsBoolean(true), JsBoolean(false), JsBoolean(true))))
    fieldType.valueToJson(Some(Array(Some(true), None, Some(true), Some(false)))) should be (JsArray(Seq(JsBoolean(true), JsNull, JsBoolean(true), JsBoolean(false))))

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsArray()).get should be (Array())
    fieldType.jsonToValue(JsArray(Seq(JsBoolean(true), JsBoolean(false), JsBoolean(true)))).get should be (Array(Some(true), Some(false), Some(true)))
    fieldType.jsonToValue(JsArray(Seq(JsBoolean(true), JsNull, JsBoolean(true), JsBoolean(false)))).get should be (Array(Some(true), None, Some(true), Some(false)))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsArray(Seq(JsBoolean(true), JsNumber(1))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(false))
    }
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
    fieldType.jsonToDisplayString(JsArray()) should be ("")
    fieldType.jsonToDisplayString(JsArray(Seq(JsBoolean(true), JsBoolean(false), JsBoolean(true)))) should be ("true,false,true")
    fieldType.jsonToDisplayString(JsArray(Seq(JsBoolean(true), JsNull, JsBoolean(true), JsBoolean(false)))) should be ("true,,true,false")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsArray(Seq(JsBoolean(true), JsNumber(1))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(false))
    }
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

  "Date array field type" should "accept only dates " in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Date, true)).asInstanceOf[FieldType[Array[Option[ju.Date]]]]
    def toDate(text: String): ju.Date =
      new SimpleDateFormat(FieldTypeHelper.displayDateFormat).parse(text)

    val date1String = "2016-08-08 20:11:55"
    val date1 = toDate(date1String)

    val date2String = "08.08.2016"
    val date2OutputString = "2016-08-08 00:00:00"
    val date2 = toDate(date2OutputString)

    val date3String = "01.10.1995 20:12"
    val date3OutputString = "1995-10-01 20:12:00"
    val date3 = toDate(date3OutputString)

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue(",").get should be (Array(None, None))
    fieldType.displayStringToValue(date1String).get should be (Array(Some(date1)))
    fieldType.displayStringToValue(date2String).get should be (Array(Some(date2)))
    fieldType.displayStringToValue(s"$date1String, $date2String, $date3String").get should be (Array(Some(date1), Some(date2), Some(date3)))
    fieldType.displayStringToValue(s"$date2String,,").get should be (Array(Some(date2), None, None))
    fieldType.displayStringToValue("1251237600000").get should be (Array(Some(new java.util.Date(1251237600000l))))
    fieldType.displayStringToValue(date1.getTime.toString).get should be (Array(Some(date1)))

    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("581251237600000")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsArray()).get should be (Array())
    fieldType.displayJsonToValue(JsArray(Seq(JsNull, JsNull))).get should be (Array(None, None))
    fieldType.displayJsonToValue(JsArray(Seq(JsString(date1String)))).get should be (Array(Some(date1)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString(date2String)))).get should be (Array(Some(date2)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString(date1String), JsString(date2String), JsString(date3String)))).get should be (Array(Some(date1), Some(date2), Some(date3)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString(date2String), JsNull, JsNull))).get should be (Array(Some(date2), None, None))
    fieldType.displayJsonToValue(JsString(date2String)).get should be (Array(Some(date2)))
    fieldType.displayJsonToValue(JsString(s"$date3String,,$date1String")).get should be (Array(Some(date3), None, Some(date1)))
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(date1.getTime)))).get should be (Array(Some(date1)))

    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsNumber(581251237600000l))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsNumber(date3.getTime))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson(",") should be (JsArray(Seq(JsNull, JsNull)))
    fieldType.displayStringToJson(date1String) should be (JsArray(Seq(JsNumber(date1.getTime))))
    fieldType.displayStringToJson(date2String) should be (JsArray(Seq(JsNumber(date2.getTime))))
    fieldType.displayStringToJson(s"$date1String, $date2String, $date3String") should be (JsArray(Seq(JsNumber(date1.getTime), JsNumber(date2.getTime), JsNumber(date3.getTime))))
    fieldType.displayStringToJson(s"$date2String,,") should be (JsArray(Seq(JsNumber(date2.getTime), JsNull, JsNull)))
    fieldType.displayStringToJson("1251237600000") should be (JsArray(Seq(JsNumber(1251237600000l))))
    fieldType.displayStringToJson(date1.getTime.toString) should be (JsArray(Seq(JsNumber(date1.getTime))))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("581251237600000")
    }

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(Array())) should be ("")
    fieldType.valueToDisplayString(Some(Array(None, None))) should be (",")
    fieldType.valueToDisplayString(Some(Array(Some(date1)))) should be (date1String)
    fieldType.valueToDisplayString(Some(Array(Some(date2)))) should be (date2OutputString)
    fieldType.valueToDisplayString(Some(Array(Some(date1), Some(date2), Some(date3)))) should be (s"$date1String,$date2OutputString,$date3OutputString")
    fieldType.valueToDisplayString(Some(Array(Some(date2), None, None))) should be (s"$date2OutputString,,")

    fieldType.valueToJson(None) should be (JsNull)
    fieldType.valueToJson(Some(Array())) should be (JsArray())
    fieldType.valueToJson(Some(Array(None, None))) should be (JsArray(Seq(JsNull, JsNull)))
    fieldType.valueToJson(Some(Array(Some(date1)))) should be (JsArray(Seq(JsNumber(date1.getTime))))
    fieldType.valueToJson(Some(Array(Some(date2)))) should be (JsArray(Seq(JsNumber(date2.getTime))))
    fieldType.valueToJson(Some(Array(Some(date1), Some(date2), Some(date3)))) should be (JsArray(Seq(JsNumber(date1.getTime), JsNumber(date2.getTime), JsNumber(date3.getTime))))
    fieldType.valueToJson(Some(Array(Some(date2), None, None))) should be (JsArray(Seq(JsNumber(date2.getTime), JsNull, JsNull)))

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsArray()).get should be (Array())
    fieldType.jsonToValue(JsArray(Seq(JsNull, JsNull))).get should be (Array(None, None))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(date1.getTime)))).get should be (Array(Some(date1)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(date2.getTime)))).get should be (Array(Some(date2)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(date1.getTime), JsNumber(date2.getTime), JsNumber(date3.getTime)))).get should be (Array(Some(date1), Some(date2), Some(date3)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(date2.getTime), JsNull, JsNull))).get should be (Array(Some(date2), None, None))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsArray(Seq(JsString(date1String))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(date3.getTime))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString(date2String))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(54))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(JsArray()) should be ("")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNull, JsNull))) should be (",")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(date1.getTime)))) should be (date1String)
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(date2.getTime)))) should be (date2OutputString)
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(date1.getTime), JsNumber(date2.getTime), JsNumber(date3.getTime)))) should be (s"$date1String,$date2OutputString,$date3OutputString")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(date2.getTime), JsNull, JsNull))) should be (s"$date2OutputString,,")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsArray(Seq(JsString(date1String))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(date3.getTime))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString(date2String))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("aa"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(54))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }
  }

  "Enum array field type" should "accept only enums " in {
    val enumValues = Map(0 -> "Male", 1 -> "Female", 2 -> "Both", 3 -> "None")
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Enum, true, enumValues)).asInstanceOf[FieldType[Array[Option[Int]]]]

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue(",").get should be (Array(None, None))
    fieldType.displayStringToValue(" Male").get should be (Array(Some(0)))
    fieldType.displayStringToValue("Both").get should be (Array(Some(2)))
    fieldType.displayStringToValue("Male,Both,  None").get should be (Array(Some(0), Some(2), Some(3)))
    fieldType.displayStringToValue("Female, ,Female").get should be (Array(Some(1), None, Some(1)))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("true")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("1")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("Female,a")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsArray()).get should be (Array())
    fieldType.displayJsonToValue(JsArray(Seq(JsNull, JsNull))).get should be (Array(None, None))
    fieldType.displayJsonToValue(JsArray(Seq(JsString(" Male")))).get should be (Array(Some(0)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("Both")))).get should be (Array(Some(2)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("Male"), JsString("Both"), JsString("None")))).get should be (Array(Some(0), Some(2), Some(3)))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("Female"), JsNull, JsString("Female")))).get should be (Array(Some(1), None, Some(1)))
    fieldType.displayJsonToValue(JsString(" Male")).get should be (Array(Some(0)))
    fieldType.displayJsonToValue(JsString("Both")).get should be (Array(Some(2)))
    fieldType.displayJsonToValue(JsString("Male,Both,  None")).get should be (Array(Some(0), Some(2), Some(3)))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsNumber(1))
    }
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
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsArray(Seq(JsNumber(1))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsArray(Seq(JsString("Female"), JsString("a"))))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson(",") should be (JsArray(Seq(JsNull, JsNull)))
    fieldType.displayStringToJson(" Male") should be (JsArray(Seq(JsNumber(0))))
    fieldType.displayStringToJson("Both") should be (JsArray(Seq(JsNumber(2))))
    fieldType.displayStringToJson("Male,Both,  None") should be (JsArray(Seq(JsNumber(0), JsNumber(2), JsNumber(3))))
    fieldType.displayStringToJson("Female, ,Female") should be (JsArray(Seq(JsNumber(1), JsNull, JsNumber(1))))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("aa")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("true")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("1")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToJson("Female,a")
    }

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(Array())) should be ("")
    fieldType.valueToDisplayString(Some(Array(None))) should be ("")
    fieldType.valueToDisplayString(Some(Array(None, None))) should be (",")
    fieldType.valueToDisplayString(Some(Array(Some(0)))) should be ("Male")
    fieldType.valueToDisplayString(Some(Array(Some(1)))) should be ("Female")
    fieldType.valueToDisplayString(Some(Array(Some(2)))) should be ("Both")
    fieldType.valueToDisplayString(Some(Array(Some(3)))) should be ("None")
    fieldType.valueToDisplayString(Some(Array(Some(0), Some(2), Some(3)))) should be ("Male,Both,None")
    fieldType.valueToDisplayString(Some(Array(Some(1), None, Some(1)))) should be ("Female,,Female")
    a [AdaConversionException] should be thrownBy {
      fieldType.valueToDisplayString(Some(Array(Some(4))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.valueToDisplayString(Some(Array(Some(1), None, Some(-1))))
    }

    fieldType valueToJson None should be (JsNull)
    fieldType.valueToJson(Some(Array())) should be (JsArray())
    fieldType.valueToJson(Some(Array(None))) should be (JsArray(Seq(JsNull)))
    fieldType.valueToJson(Some(Array(None, None))) should be (JsArray(Seq(JsNull, JsNull)))
    fieldType.valueToJson(Some(Array(Some(0)))) should be (JsArray(Seq(JsNumber(0))))
    fieldType.valueToJson(Some(Array(Some(1)))) should be (JsArray(Seq(JsNumber(1))))
    fieldType.valueToJson(Some(Array(Some(2)))) should be (JsArray(Seq(JsNumber(2))))
    fieldType.valueToJson(Some(Array(Some(3)))) should be (JsArray(Seq(JsNumber(3))))
    fieldType.valueToJson(Some(Array(Some(0), Some(2), Some(3)))) should be (JsArray(Seq(JsNumber(0), JsNumber(2), JsNumber(3))))
    fieldType.valueToJson(Some(Array(Some(1), None, Some(1)))) should be (JsArray(Seq(JsNumber(1), JsNull, JsNumber(1))))
    a [AdaConversionException] should be thrownBy {
      fieldType.valueToJson(Some(Array(Some(4))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.valueToJson(Some(Array(Some(1), None, Some(-1))))
    }

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsArray()).get should be (Array())
    fieldType.jsonToValue(JsArray(Seq(JsNull))).get should be (Array(None))
    fieldType.jsonToValue(JsArray(Seq(JsNull, JsNull))).get should be (Array(None, None))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(0)))).get should be (Array(Some(0)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(1)))).get should be (Array(Some(1)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(2)))).get should be (Array(Some(2)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(3)))).get should be (Array(Some(3)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(0), JsNumber(2), JsNumber(3)))).get should be (Array(Some(0), Some(2), Some(3)))
    fieldType.jsonToValue(JsArray(Seq(JsNumber(2), JsNull, JsNumber(2)))).get should be (Array(Some(2), None, Some(2)))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(3))
    }
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
      fieldType.jsonToValue(JsString("Male"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsArray(Seq(JsString("Male"))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(true))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(JsArray()) should be ("")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNull))) should be ("")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNull, JsNull))) should be (",")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(0)))) should be ("Male")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(1)))) should be ("Female")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(2)))) should be ("Both")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(3)))) should be ("None")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(0), JsNumber(2), JsNumber(3)))) should be ("Male,Both,None")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(1), JsNull, JsNumber(1)))) should be ("Female,,Female")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(3))
    }
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
      fieldType.jsonToDisplayString(JsString("Male"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsArray(Seq(JsString("Male"))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(true))
    }
  }

  "String array field type" should "accept only strings " in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.String, true)).asInstanceOf[FieldType[Array[Option[String]]]]

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue(null) should be (None)
    fieldType.displayStringToValue(" Male").get should be (Array(Some("Male")))
    fieldType.displayStringToValue("Lala rerwepn. fdso").get should be (Array(Some("Lala rerwepn. fdso")))
    fieldType.displayStringToValue("123").get should be (Array(Some("123")))
    fieldType.displayStringToValue("true").get should be (Array(Some("true")))
    fieldType.displayStringToValue("Lux,Ger, Fr").get should be (Array(Some("Lux"), Some("Ger"), Some("Fr")))
    fieldType.displayStringToValue(",,pen,Paper,").get should be (Array(None, None, Some("pen"), Some("Paper"), None))

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsArray()).get should be (Array())
    fieldType.displayJsonToValue(JsArray(Seq(JsNull))).get should be (Array(None))
    fieldType.displayJsonToValue(JsArray(Seq(JsString(" Male")))).get should be (Array(Some("Male")))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("Lala rerwepn. fdso")))).get should be (Array(Some("Lala rerwepn. fdso")))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("123")))).get should be (Array(Some("123")))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("true")))).get should be (Array(Some("true")))
    fieldType.displayJsonToValue(JsArray(Seq(JsNumber(1)))).get should be (Array(Some("1")))
    fieldType.displayJsonToValue(JsArray(Seq(JsBoolean(true)))).get should be (Array(Some("true")))
    fieldType.displayJsonToValue(JsArray(Seq(JsString("Lux"), JsString("Ger"), JsString("Fr")))).get should be (Array(Some("Lux"), Some("Ger"), Some("Fr")))
    fieldType.displayJsonToValue(JsArray(Seq(JsNull, JsNull, JsString("pen"), JsString("Paper"), JsNull))).get should be (Array(None, None, Some("pen"), Some("Paper"), None))
    fieldType.displayJsonToValue(JsString("Lala")).get should be (Array(Some("Lala")))
    fieldType.displayJsonToValue(JsString("Lux,Ger,, Fr")).get should be (Array(Some("Lux"), Some("Ger"), None, Some("Fr")))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsNumber(1))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }

    fieldType.displayStringToJson("") should be (JsNull)
    fieldType.displayStringToJson(null) should be (JsNull)
    fieldType.displayStringToJson(" Male") should be (JsArray(Seq(JsString("Male"))))
    fieldType.displayStringToJson("Lala rerwepn. fdso") should be (JsArray(Seq(JsString("Lala rerwepn. fdso"))))
    fieldType.displayStringToJson("123") should be (JsArray(Seq(JsString(("123")))))
    fieldType.displayStringToJson("true") should be (JsArray(Seq(JsString(("true")))))
    fieldType.displayStringToJson("Lux,Ger, Fr") should be (JsArray(Seq(JsString("Lux"), JsString("Ger"), JsString("Fr"))))
    fieldType.displayStringToJson(",,pen,Paper,") should be (JsArray(Seq(JsNull, JsNull, JsString("pen"), JsString("Paper"), JsNull)))

    fieldType.valueToDisplayString(None) should be ("")
    fieldType.valueToDisplayString(Some(Array())) should be ("")
    fieldType.valueToDisplayString(Some(Array(None))) should be ("")
    fieldType.valueToDisplayString(Some(Array(None, None))) should be (",")
    fieldType.valueToDisplayString(Some(Array(Some(" Male")))) should be (" Male")
    fieldType.valueToDisplayString(Some(Array(Some("Lala rerwepn. fdso")))) should be ("Lala rerwepn. fdso")
    fieldType.valueToDisplayString(Some(Array(Some("123")))) should be ("123")
    fieldType.valueToDisplayString(Some(Array(Some("true")))) should be ("true")
    fieldType.valueToDisplayString(Some(Array(Some("Lux"), Some("Ger"), Some("Fr")))) should be ("Lux,Ger,Fr")
    fieldType.valueToDisplayString(Some(Array(None, None, Some("pen"), Some("Paper"), None))) should be (",,pen,Paper,")

    fieldType.valueToJson(None) should be (JsNull)
    fieldType.valueToJson(Some(Array())) should be (JsArray())
    fieldType.valueToJson(Some(Array(None))) should be (JsArray(Seq(JsNull)))
    fieldType.valueToJson(Some(Array(None, None))) should be (JsArray(Seq(JsNull, JsNull)))
    fieldType.valueToJson(Some(Array(Some(" Male")))) should be (JsArray(Seq(JsString(" Male"))))
    fieldType.valueToJson(Some(Array(Some("Lala rerwepn. fdso")))) should be (JsArray(Seq(JsString("Lala rerwepn. fdso"))))
    fieldType.valueToJson(Some(Array(Some("123")))) should be (JsArray(Seq(JsString("123"))))
    fieldType.valueToJson(Some(Array(Some("true")))) should be (JsArray(Seq(JsString("true"))))
    fieldType.valueToJson(Some(Array(Some("Lux"), Some("Ger"), Some("Fr")))) should be (JsArray(Seq(JsString("Lux"), JsString("Ger"), JsString("Fr"))))
    fieldType.valueToJson(Some(Array(None, None, Some("pen"), Some("Paper"), None))) should be (JsArray(Seq(JsNull, JsNull, JsString("pen"), JsString("Paper"), JsNull)))

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsArray()).get should be (Array())
    fieldType.jsonToValue(JsArray(Seq(JsNull))).get should be (Array(None))
    fieldType.jsonToValue(JsArray(Seq(JsString(" Male")))).get should be (Array(Some(" Male")))
    fieldType.jsonToValue(JsArray(Seq(JsString("Lala rerwepn. fdso")))).get should be (Array(Some("Lala rerwepn. fdso")))
    fieldType.jsonToValue(JsArray(Seq(JsString("123")))).get should be (Array(Some("123")))
    fieldType.jsonToValue(JsArray(Seq(JsString("true")))).get should be (Array(Some("true")))
    fieldType.jsonToValue(JsArray(Seq(JsString("Lux"), JsString("Ger"), JsString("Fr")))).get should be (Array(Some("Lux"), Some("Ger"), Some("Fr")))
    fieldType.jsonToValue(JsArray(Seq(JsNull, JsNull, JsString("pen"), JsString("Paper"), JsNull))).get should be (Array(None, None, Some("pen"), Some("Paper"), None))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsArray(Seq(JsBoolean(true))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsArray(Seq(JsNumber(1))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("Lala"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(1))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(JsArray()) should be ("")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNull))) should be ("")
    fieldType.jsonToDisplayString(JsArray(Seq(JsString(" Male")))) should be (" Male")
    fieldType.jsonToDisplayString(JsArray(Seq(JsString("Lala rerwepn. fdso")))) should be ("Lala rerwepn. fdso")
    fieldType.jsonToDisplayString(JsArray(Seq(JsString("123")))) should be ("123")
    fieldType.jsonToDisplayString(JsArray(Seq(JsString("true")))) should be ("true")
    fieldType.jsonToDisplayString(JsArray(Seq(JsString("Lux"), JsString("Ger"), JsString("Fr")))) should be ("Lux,Ger,Fr")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNull, JsNull, JsString("pen"), JsString("Paper"), JsNull))) should be (",,pen,Paper,")
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsArray(Seq(JsBoolean(true))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsArray(Seq(JsNumber(1))))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("Lala"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(1))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(true))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB")))
    }
  }

  "JSON array field type" should "accept only JSONS" in {
    val fieldType = ftf.apply(FieldTypeSpec(FieldTypeId.Json, true)).asInstanceOf[FieldType[Array[Option[JsObject]]]]

    val json1 = Json.obj("name" -> JsString("Peter"), "affiliation" -> JsString("LCSB"))
    val json1String = "{\"name\":\"Peter\",\"affiliation\":\"LCSB\"}"

    val json2 = Json.obj("name" -> JsString("John"), "affiliation" -> JsString("MIT"))
    val json2String = "{\"name\":\"John\",\"affiliation\":\"MIT\"}"

    val jsonArray1 = JsArray(Seq(json1))
    val jsonArray1String = s"[$json1String]"

    val jsonArray2 = JsArray(Seq(json1, json2))
    val jsonArray2String = s"[$json1String,$json2String]"

    val jsonArray3 = JsArray(Seq(JsNull, json1))
    val jsonArray3String = s"[null,$json1String]"

    fieldType.displayStringToValue("") should be (None)
    fieldType.displayStringToValue(null) should be (None)
    fieldType.displayStringToValue("[]").get should be (Array())
    fieldType.displayStringToValue("[null]").get should be (Array(None))
    fieldType.displayStringToValue(json1String).get should be (Array(Some(json1)))
    fieldType.displayStringToValue(json2String).get should be (Array(Some(json2)))
    fieldType.displayStringToValue(jsonArray1String).get should be (Array(Some(json1)))
    fieldType.displayStringToValue(jsonArray2String).get should be (Array(Some(json1), Some(json2)))
    fieldType.displayStringToValue(jsonArray3String).get should be (Array(None, Some(json1)))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("123")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("true")
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.displayStringToValue("Lala")
    }

    fieldType.displayJsonToValue(JsNull) should be (None)
    fieldType.displayJsonToValue(JsArray()).get should be (Array())
    fieldType.displayJsonToValue(JsArray(Seq(JsNull))).get should be (Array(None))
    fieldType.displayJsonToValue(JsArray(Seq(JsNull))).get should be (Array(None))
    fieldType.displayJsonToValue(JsArray(Seq(JsNull, JsNull))).get should be (Array(None, None))
    fieldType.displayJsonToValue(jsonArray1).get should be (Array(Some(json1)))
    fieldType.displayJsonToValue(jsonArray2).get should be (Array(Some(json1), Some(json2)))
    fieldType.displayJsonToValue(jsonArray3).get should be (Array(None, Some(json1)))
    fieldType.displayJsonToValue(json1).get should be (Array(Some(json1)))
    a [AdaConversionException] should be thrownBy {
      fieldType.displayJsonToValue(JsString(json1String))
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
    fieldType.displayStringToJson(null) should be (JsNull)
    fieldType.displayStringToJson(json1String) should be (JsArray(Seq(json1)))
    fieldType.displayStringToJson(json2String) should be (JsArray(Seq(json2)))
    fieldType.displayStringToJson(jsonArray1String) should be (jsonArray1)
    fieldType.displayStringToJson(jsonArray2String) should be (jsonArray2)
    fieldType.displayStringToJson(jsonArray3String) should be (jsonArray3)
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
    fieldType.valueToDisplayString(Some(Array())) should be ("[]")
    fieldType.valueToDisplayString(Some(Array(None))) should be ("[null]")
    fieldType.valueToDisplayString(Some(Array(None, None))) should be ("[null,null]")
    fieldType.valueToDisplayString(Some(Array(Some(json2)))) should be (s"[$json2String]")
    fieldType.valueToDisplayString(Some(Array(Some(json1)))) should be (jsonArray1String)
    fieldType.valueToDisplayString(Some(Array(Some(json1), Some(json2)))) should be (jsonArray2String)
    fieldType.valueToDisplayString(Some(Array(None, Some(json1)))) should be (jsonArray3String)

    fieldType.valueToJson(None) should be (JsNull)
    fieldType.valueToJson(Some(Array())) should be (JsArray())
    fieldType.valueToJson(Some(Array(None))) should be (JsArray(Seq(JsNull)))
    fieldType.valueToJson(Some(Array(None, None))) should be (JsArray(Seq(JsNull, JsNull)))
    fieldType.valueToJson(Some(Array(Some(json1)))) should be (JsArray(Seq(json1)))
    fieldType.valueToJson(Some(Array(Some(json2)))) should be (JsArray(Seq(json2)))
    fieldType.valueToJson(Some(Array(Some(json1), Some(json2)))) should be (jsonArray2)
    fieldType.valueToJson(Some(Array(None, Some(json1)))) should be (jsonArray3)

    fieldType.jsonToValue(JsNull) should be (None)
    fieldType.jsonToValue(JsArray()).get should be (Array())
    fieldType.jsonToValue(JsArray(Seq(JsNull))).get should be (Array(None))
    fieldType.jsonToValue(JsArray(Seq(JsNull, JsNull))).get should be (Array(None, None))
    fieldType.jsonToValue(jsonArray1).get should be (Array(Some(json1)))
    fieldType.jsonToValue(jsonArray2).get should be (Array(Some(json1), Some(json2)))
    fieldType.jsonToValue(jsonArray3).get should be (Array(None, Some(json1)))
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString(json1String))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(json1)
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsString("Lala"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsNumber(123))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToValue(JsBoolean(true))
    }

    fieldType.jsonToDisplayString(JsNull) should be ("")
    fieldType.jsonToDisplayString(JsArray()) should be ("[]")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNull))) should be ("[null]")
    fieldType.jsonToDisplayString(JsArray(Seq(JsNull, JsNull))) should be ("[null,null]")
    fieldType.jsonToDisplayString(jsonArray1) should be (jsonArray1String)
    fieldType.jsonToDisplayString(jsonArray2) should be (jsonArray2String)
    fieldType.jsonToDisplayString(jsonArray3) should be (jsonArray3String)
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString(json1String))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(json1)
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsString("Lala"))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsNumber(123))
    }
    a [AdaConversionException] should be thrownBy {
      fieldType.jsonToDisplayString(JsBoolean(true))
    }
  }
}