package org.ada.server.calc.json

import org.ada.server.models.{Field, FieldTypeId}
import play.api.libs.json.JsObject
import org.ada.server.calc.impl.JsonFieldUtil._
import org.ada.server.calc.impl._

import scala.reflect.runtime.universe._

////////////////////////
// General Converters //
////////////////////////

class AnyScalarConverter extends ScalarConverter[Any]

class AnyArrayConverter extends ArrayConverter[Any]
class IntArrayConverter extends ArrayConverter[Int]
class DoubleArrayConverter extends ArrayConverter[Double]
class LongArrayConverter extends ArrayConverter[Long]
class DateArrayConverter extends ArrayConverter[java.util.Date]
class StringArrayConverter extends ArrayConverter[String]
class BooleanArrayConverter extends ArrayConverter[Boolean]

class AnyTupleConverter extends TupleConverter[Any, Any]
class AnyArrayTupleConverter extends ArrayTupleConverter[Any, Any]
class AnyTuple3Converter extends Tuple3Converter[Any, Any, Any]
class AnySeqConverter extends SeqConverter[Any]

////////////////////////////////////
// Calculator Specific Converters //
////////////////////////////////////

class ScalarNumericDistributionCountsConverter extends ScalarDoubleConverter {
  override def specificUseClass = Some(NumericDistributionCountsCalcAux.getClass)
}

class ArrayNumericDistributionCountsConverter extends ArrayDoubleConverter {
  override def specificUseClass = Some(NumericDistributionCountsCalcAux.getClass)
}

class ScalarGroupNumericDistributionCountsConverter extends ScalarGroupDoubleConverter[Any] {
  override def specificUseClass = Some(classOf[GroupNumericDistributionCountsCalc[Any]])
}

class ArrayGroupNumericDistributionCountsConverter extends ArrayGroupDoubleConverter[Any] {
  override def specificUseClass = Some(classOf[GroupNumericDistributionCountsCalc[Any]])
}

class ScalarCumulativeNumericBinCountsConverter extends ScalarDoubleConverter {
  override def specificUseClass = Some(classOf[CumulativeNumericBinCountsCalc])
}

class ArrayCumulativeNumericBinCountsConverter extends ArrayDoubleConverter {
  override def specificUseClass = Some(classOf[CumulativeNumericBinCountsCalc])
}

class ScalarGroupCumulativeNumericBinCountsConverter extends ScalarGroupDoubleConverter[Any] {
  override def specificUseClass = Some(classOf[GroupCumulativeNumericBinCountsCalc[Any]])
}

class ArrayGroupCumulativeNumericBinCountsConverter extends ArrayGroupDoubleConverter[Any] {
  override def specificUseClass = Some(classOf[GroupNumericDistributionCountsCalc[Any]])
}

class AnyStringGroupTupleConverter extends StringGroupTupleConverter[Any, Any] {
  override def specificUseClass = Some(classOf[GroupTupleCalc[String, _, _]])
}

class AnyStringUniqueGroupTupleConverter extends StringGroupTupleConverter[Any, Any] {
  override def specificUseClass = Some(classOf[GroupUniqueTupleCalc[String, _, _]])
}

class BasicStatsConverter extends ScalarDoubleConverter {
  override def specificUseClass = Some(BasicStatsCalc.getClass)
}

class ArrayBasicStatsConverter extends ArrayDoubleConverter {
  override def specificUseClass = Some(BasicStatsCalc.getClass)
}

class MultiBasicStatsConverter extends SeqDoubleConverter {
  override def specificUseClass = Some(MultiBasicStatsCalc.getClass)
}

class CountDistinctConverter extends ScalarDoubleConverter {
  override def specificUseClass = Some(classOf[CountDistinctCalc[Any]])
}

class ArrayCountDistinctConverter extends ArrayDoubleConverter {
  override def specificUseClass = Some(classOf[CountDistinctCalc[Any]])
}

class PearsonCorrelationConverter extends SeqDoubleConverter {
  override def specificUseClass = Some(PearsonCorrelationCalc.getClass)
}

class AllDefinedPearsonCorrelationConverter extends AllDefinedSeqDoubleConverter {
  override def specificUseClass = Some(AllDefinedPearsonCorrelationCalc.getClass)
}

class MatthewsBinaryClassCorrelationConverter extends SeqBooleanConverter {
  override def specificUseClass = Some(MatthewsBinaryClassCorrelationCalc.getClass)
}

class EuclideanDistanceCalcConverter extends SeqDoubleConverter {
  override def specificUseClass = Some(EuclideanDistanceCalc.getClass)
}

class AllDefinedEuclideanDistanceCalcConverter extends AllDefinedSeqDoubleConverter {
  override def specificUseClass = Some(AllDefinedEuclideanDistanceCalc.getClass)
}

class SeqBinMeanCalcConverter extends SeqDoubleConverter {
  override def specificUseClass = Some(SeqBinMeanCalcAux.getClass)
}

class AllDefinedSeqBinMeanCalcConverter extends AllDefinedSeqDoubleConverter {
  override def specificUseClass = Some(classOf[AllDefinedSeqBinMeanCalc])
}

class SeqBinMaxCalcConverter extends SeqDoubleConverter {
  override def specificUseClass = Some(SeqBinMaxCalcAux.getClass)
}

class AllDefinedSeqBinMaxCalcConverter extends AllDefinedSeqDoubleConverter {
  override def specificUseClass = Some(classOf[AllDefinedSeqBinMaxCalc])
}

class SeqBinMinCalcConverter extends SeqDoubleConverter {
  override def specificUseClass = Some(SeqBinMinCalcAux.getClass)
}

class AllDefinedSeqBinMinCalcConverter extends AllDefinedSeqDoubleConverter {
  override def specificUseClass = Some(classOf[AllDefinedSeqBinMinCalc])
}

class SeqBinVarianceCalcConverter extends SeqDoubleConverter {
  override def specificUseClass = Some(SeqBinVarianceCalcAux.getClass)
}

class AllDefinedSeqBinVarianceCalcConverter extends AllDefinedSeqDoubleConverter {
  override def specificUseClass = Some(classOf[AllDefinedSeqBinVarianceCalc])
}

class SeqBinCountCalcConverter extends SeqDoubleConverter {
  override def specificUseClass = Some(SeqBinCountCalcAux.getClass)
}

class AllDefinedSeqBinCountCalcConverter extends AllDefinedSeqDoubleConverter {
  override def specificUseClass = Some(classOf[AllDefinedSeqBinCountCalc])
}

class OneWayAnovaTestConverter extends GroupScalarDoubleConverter[Any] {
  override def specificUseClass = Some(classOf[OneWayAnovaTestCalc[_]])
}

class MultiOneWayAnovaTestConverter extends GroupSeqDoubleConverter[Any] {
  override def specificUseClass = Some(classOf[MultiOneWayAnovaTestCalc[_]])
}

class NullExcludedMultiOneWayAnovaTestConverter extends GroupSeqDoubleConverter[Any] {
  override def specificUseClass = Some(classOf[NullExcludedMultiOneWayAnovaTestCalc[_]])
}

class MultiChiSquareTestConverter extends GroupSeqConverter[Any, Any] {
  override def specificUseClass = Some(classOf[MultiChiSquareTestCalc[_, _]])
}

class NullExcludedMultiChiSquareTestConverter extends GroupSeqConverter[Any, Any] {
  override def specificUseClass = Some(classOf[NullExcludedMultiChiSquareTestCalc[_, _]])
}

////////////////////
// Helper Classes //
////////////////////

private[json] abstract class ScalarConverter[T: TypeTag] extends JsonInputConverter[Option[T]] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 1)
    jsonToValue[T](fields(0))
  }

  override def inputType = typeOf[Option[T]]
}

private[json] abstract class ScalarDoubleConverter extends JsonInputConverter[Option[Double]] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 1)
    jsonToDouble(fields(0))
  }

  override def inputType = typeOf[Option[Double]]
}

private[json] abstract class ArrayConverter[T: TypeTag] extends JsonInputConverter[Array[Option[T]]] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 1)
    jsonToArrayValue[T](fields(0))
  }

  override def inputType = typeOf[Array[Option[T]]]
}

private[json] abstract class SeqConverter[T: TypeTag] extends JsonInputConverter[Seq[Option[T]]] {

  override def apply(fields: Seq[Field]) =
    jsonToValues(fields)

  override def inputType = typeOf[Seq[Option[T]]]
}

private[json] abstract class ArrayDoubleConverter extends JsonInputConverter[Array[Option[Double]]] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 1)
    jsonToArrayDouble(fields(0))
  }

  override def inputType = typeOf[Array[Option[Double]]]
}

private[json] abstract class GroupScalarDoubleConverter[G: TypeTag] extends JsonInputConverter[(Option[G], Option[Double])] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 2)

    val groupConverter = jsonToValue[G](fields(0))
    val valueConverter = jsonToDouble(fields(1))

    (jsObject: JsObject) =>
      (groupConverter(jsObject), valueConverter(jsObject))
  }

  override def inputType = typeOf[(Option[G], Option[Double])]
}

private[json] abstract class SeqDoubleConverter extends JsonInputConverter[Seq[Option[Double]]] {

  override def apply(fields: Seq[Field]) =
    jsonToDoubles(fields)

  override def inputType = typeOf[Seq[Option[Double]]]
}

private[json] abstract class GroupSeqDoubleConverter[G: TypeTag] extends JsonInputConverter[(Option[G], Seq[Option[Double]])] {

  override def apply(fields: Seq[Field]) = {
    checkFieldsMin(fields, 1)

    val groupConverter = jsonToValue[G](fields(0))
    val valuesConverter = jsonToDoubles(fields.tail)

    (jsObject: JsObject) =>
      (groupConverter(jsObject), valuesConverter(jsObject))
  }

  override def inputType = typeOf[(Option[G], Seq[Option[Double]])]
}

private[json] abstract class SeqBooleanConverter extends JsonInputConverter[Seq[Option[Boolean]]] {

  override def apply(fields: Seq[Field]) =
    jsonToBooleans(fields)

  override def inputType = typeOf[Seq[Option[Boolean]]]
}

private[json] abstract class GroupSeqConverter[G: TypeTag, T: TypeTag] extends JsonInputConverter[(Option[G], Seq[Option[T]])] {

  override def apply(fields: Seq[Field]) = {
    checkFieldsMin(fields, 1)

    val groupConverter = jsonToValue[G](fields(0))
    val valuesConverter = jsonToValues(fields.tail)

    (jsObject: JsObject) =>
      (groupConverter(jsObject), valuesConverter(jsObject))
  }

  override def inputType = typeOf[(Option[G], Seq[Option[T]])]
}

private[json] abstract class GroupDefinedSeqConverter[G: TypeTag, T: TypeTag] extends JsonInputConverter[(G, Seq[Option[T]])] {

  override def apply(fields: Seq[Field]) = {
    checkFieldsMin(fields, 1)

    val groupConverter = jsonToDefinedValue[G](fields(0))
    val valuesConverter = jsonToValues(fields.tail)

    (jsObject: JsObject) =>
      (groupConverter(jsObject), valuesConverter(jsObject))
  }

  override def inputType = typeOf[(G, Seq[Option[T]])]
}

private[json] abstract class AllDefinedSeqDoubleConverter extends JsonInputConverter[Seq[Double]] {

  override def apply(fields: Seq[Field]) =
    jsonToDoublesDefined(fields)

  override def inputType = typeOf[Seq[Double]]
}

private[json] abstract class TupleConverter[T1: TypeTag, T2: TypeTag] extends JsonInputConverter[(Option[T1], Option[T2])] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 2)
    jsonToTuple[T1, T2](fields(0), fields(1))
  }

  override def inputType = typeOf[(Option[T1], Option[T2])]
}

private[json] abstract class Tuple3Converter[T1: TypeTag, T2: TypeTag, T3: TypeTag] extends JsonInputConverter[(Option[T1], Option[T2], Option[T3])] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 3)
    jsonToTuple[T1, T2, T3](fields(0), fields(1), fields(2))
  }

  override def inputType = typeOf[(Option[T1], Option[T2], Option[T3])]
}

private[json] abstract class ScalarGroupDoubleConverter[G: TypeTag] extends JsonInputConverter[(Option[G], Option[Double])] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 2)

    val groupConverter = jsonToValue(fields(0))
    val doubleConverter = jsonToDouble(fields(1))
    (jsObject: JsObject) =>
      (groupConverter(jsObject), doubleConverter(jsObject))
  }

  override def inputType = typeOf[(Option[G], Option[Double])]
}

private[json] abstract class ArrayTupleConverter[T1: TypeTag, T2: TypeTag] extends JsonInputConverter[Array[(Option[T1], Option[T2])]] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 2)

    val groupConverter = jsonToValue[T1](fields(0))
    val arrayValueConverter = jsonToArrayValue[T2](fields(1))

    (jsObject: JsObject) =>
      val group = groupConverter(jsObject)
      val array = arrayValueConverter(jsObject)
      array.map((group, _))
  }

  override def inputType = typeOf[Array[(Option[T1], Option[T2])]]
}

private[json] abstract class ArrayGroupDoubleConverter[G: TypeTag] extends JsonInputConverter[Array[(Option[G], Option[Double])]] {

  override def apply(fields: Seq[Field]) = {
    val groupConverter = jsonToValue[G](fields(0))
    val doubleConverter = jsonToArrayDouble(fields(1))

    (jsObject: JsObject) =>
      val group = groupConverter(jsObject)
      val array = doubleConverter(jsObject)
      array.map((group, _))
  }

  override def inputType = typeOf[Array[(Option[G], Option[Double])]]
}

private[json] abstract class ScalarNumericConverter[T: TypeTag] extends JsonInputConverter[Option[T]] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 1)

    jsonToNumericValue[T](fields(0))
  }

  override def inputType = typeOf[Option[T]]
}

private[json] abstract class ArrayNumericConverter[T: TypeTag] extends JsonInputConverter[Array[Option[T]]] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 1)

    jsonToArrayNumericValue[T](fields(0))
  }

  override def inputType = typeOf[Array[Option[T]]]
}

private[json] abstract class GroupScalarNumericConverter[G: TypeTag, T: TypeTag] extends JsonInputConverter[(Option[G], Option[T])] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 2)

    val groupConverter = jsonToValue[G](fields(0))
    val converter = jsonToNumericValue[T](fields(1))

    (jsObject: JsObject) =>
      val group = groupConverter(jsObject)
      val value = converter(jsObject)
      (group, value)
  }

  override def inputType = typeOf[(Option[G], Option[T])]
}

private[json] abstract class ArrayGroupNumericConverter[G: TypeTag, T: TypeTag] extends JsonInputConverter[Array[(Option[G], Option[T])]] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 2)

    val groupConverter = jsonToValue[G](fields(0))
    val converter = jsonToArrayNumericValue[T](fields(1))

    (jsObject: JsObject) =>
      val group = groupConverter(jsObject)
      val array = converter(jsObject)
      array.map((group, _))
  }

  override def inputType = typeOf[Array[(Option[G], Option[T])]]
}

private[json] abstract class StringGroupTupleConverter[T1: TypeTag, T2: TypeTag] extends JsonInputConverter[GroupTupleCalcTypePack[String, T1, T2]#IN] {

  override def apply(fields: Seq[Field]) = {
    checkFields(fields, 3)

    // json to tuple converter
    val jsonTuple = jsonToTuple[T1, T2](fields(1), fields(2))

    // create a group->string converter and merge with the value one
    val groupJsonString = jsonToDisplayString(fields(0))

    (jsObject: JsObject) =>
      val values = jsonTuple(jsObject)
      (groupJsonString(jsObject), values._1, values._2)
  }

  override def inputType = typeOf[(Option[String], Option[T1], Option[T2])]
}