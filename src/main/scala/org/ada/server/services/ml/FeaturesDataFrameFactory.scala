package org.ada.server.services.ml

import java.{util => ju}

import org.ada.server.field.{FieldType, FieldTypeHelper}
import org.ada.server.models.ml.IOJsonTimeSeriesSpec
import org.ada.server.models.{FieldTypeId, FieldTypeSpec}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.{QuantileDiscretizer, StringIndexer, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import play.api.libs.json.{JsArray, JsObject, JsString, Json}
import org.incal.spark_ml.transformers.FixedOrderStringIndexer
import org.incal.spark_ml.SparkUtil.{prepFeaturesDataFrame, indexStringCols}

import scala.util.Random

object FeaturesDataFrameFactory {

  private val ftf = FieldTypeHelper.fieldTypeFactory()

  def apply(
    session: SparkSession,
    jsons: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    featureFieldNames: Seq[String]
  ): DataFrame = {
    // convert jsons to a data frame
    val fieldNameAndTypes = fields.map { case (name, fieldTypeSpec) => (name, ftf(fieldTypeSpec))}
    val stringFieldNames = fields.filter {_._2.fieldType == FieldTypeId.String }.map(_._1)
    val stringFieldsNotToIndex = stringFieldNames.diff(featureFieldNames).toSet
    val df = jsonsToDataFrame(session)(jsons, fieldNameAndTypes, stringFieldsNotToIndex)

    df.transform(
      prepFeaturesDataFrame(featureFieldNames.toSet, None)
    )
  }

  def apply(
    session: SparkSession,
    jsons: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)]
  ) = {
    val fieldNameAndTypes = fields.map { case (name, fieldTypeSpec) => (name, ftf(fieldTypeSpec)) }
    jsonsToDataFrame(session)(jsons, fieldNameAndTypes)
  }

  def applyWoFeatures(
    session: SparkSession,
    jsons: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    outputFieldName: Option[String],
    discretizerBucketNum: Option[Int] = None
  ): DataFrame = {
    // convert jsons to a data frame
    val fieldNameAndTypes = fields.map { case (name, fieldTypeSpec) => (name, ftf(fieldTypeSpec)) }
    val df = jsonsToDataFrame(session)(jsons, fieldNameAndTypes)

    // prep the features of the data frame
    val featureNames = featureFieldNames(fields, outputFieldName)

    val numericFieldNames = fields.flatMap { case (name, fieldTypeSpec) =>
      if (featureNames.contains(name)) {
        if (fieldTypeSpec.fieldType == FieldTypeId.Integer || fieldTypeSpec.fieldType == FieldTypeId.Double)
          Some(name)
        else
          None
      } else
        None
    }

    discretizerBucketNum.map(discretizerBucketNum =>
      numericFieldNames.foldLeft(df) { case (newDf, fieldName) =>
        discretizeAsQuantiles(newDf, discretizerBucketNum, fieldName)
      }
    ).getOrElse(df)
  }

  def apply(
    session: SparkSession,
    jsons: Traversable[JsObject],
    fields: Seq[(String, FieldTypeSpec)],
    outputFieldName: Option[String],
    discretizerBucketNum: Option[Int] = None,
    dropFeatureCols: Boolean = true,
    dropNaValues: Boolean = true
  ): DataFrame = {
    val df = applyWoFeatures(session, jsons, fields, outputFieldName, discretizerBucketNum)

    // prep the features of the data frame
    val featureNames = featureFieldNames(fields, outputFieldName)

    df.transform(
      prepFeaturesDataFrame(featureNames, outputFieldName, dropFeatureCols, dropNaValues)
    )
  }

  def applySeries(
    session: SparkSession)(
    json: JsObject,
    ioSpec: IOJsonTimeSeriesSpec,
    seriesOrderCol: String
  ): DataFrame = {
    val values: Seq[Seq[Any]] =
      IOSeriesUtil(json, ioSpec).map { case (inputs, outputs) =>
        inputs.zip(outputs).zipWithIndex.map { case ((input, output), index) => Seq(index) ++ input ++ Seq(output) }
      }.getOrElse(Nil)

    val valueBroadVar = session.sparkContext.broadcast(values)
    val size = values.size

    val data: RDD[Row] = session.sparkContext.range(0, size).map { index =>
      if (index == 0)
        println(s"Creating Spark rows from a broadcast variable of size ${size}")
      Row.fromSeq(valueBroadVar.value(index.toInt))
    }

    val inputFieldNames = ioSpec.inputSeriesFieldPaths.map( fieldName =>
      fieldName.replaceAllLiterally(".", "_")
    )

    val outputFieldName = ioSpec.outputSeriesFieldPath.replaceAllLiterally(".", "_")
    val ioTypes = (inputFieldNames ++ Seq(outputFieldName)).toSet.map { name: String => StructField(name, DoubleType, true) }
    val structTypes = Seq(StructField(seriesOrderCol, IntegerType, true)) ++ ioTypes

    val df = session.createDataFrame(data, StructType(structTypes))

    df.cache()

    df.transform(
      prepFeaturesDataFrame(inputFieldNames.toSet, Some(outputFieldName))
    )
  }

  private def featureFieldNames(
    fields: Seq[(String, FieldTypeSpec)],
    outputFieldName: Option[String]
  ): Set[String] =
    outputFieldName.map( outputName =>
      fields.map(_._1).filterNot(_ == outputName)
    ).getOrElse(
      fields.map(_._1)
    ).toSet

  private def jsonsToDataFrame(
    session: SparkSession)(
    jsons: Traversable[JsObject],
    fieldNameAndTypes: Seq[(String, FieldType[_])],
    stringFieldsNotToIndex: Set[String] = Set()
  ): DataFrame = {
    val (df, broadcastVar) = jsonsToDataFrameAux(session)(jsons, fieldNameAndTypes, stringFieldsNotToIndex)

    df.cache()
    broadcastVar.unpersist()
    df
  }

  private def jsonsToDataFrameAux(
    session: SparkSession)(
    jsons: Traversable[JsObject],
    fieldNameAndTypes: Seq[(String, FieldType[_])],
    stringFieldsNotToIndex: Set[String] = Set()
  ): (DataFrame, Broadcast[_]) = {
    val values = jsons.toSeq.map( json =>
      fieldNameAndTypes.map { case (fieldName, fieldType) =>
        fieldType.spec.fieldType match {
          case FieldTypeId.Enum =>
            fieldType.jsonToDisplayString(json \ fieldName)

          case FieldTypeId.Date =>
            fieldType.asValueOf[ju.Date].jsonToValue(json \ fieldName).map(date => new java.sql.Date(date.getTime)).getOrElse(null)

          case _ =>
            fieldType.jsonToValue(json \ fieldName).getOrElse(null)
        }
      }
    )

    val valueBroadVar = session.sparkContext.broadcast(values)
    val size = values.size

    val data: RDD[Row] = session.sparkContext.range(0, size).map { index =>
      if (index == 0)
        println(s"Creating Spark rows from a broadcast variable of size ${size}")
      Row.fromSeq(valueBroadVar.value(index.toInt))
    }

    data.cache()

    val structTypesWithEnumLabels = fieldNameAndTypes.map { case (fieldName, fieldType) =>
      val spec = fieldType.spec

      val sparkFieldType: DataType = spec.fieldType match {
        case FieldTypeId.Integer => LongType
        case FieldTypeId.Double => DoubleType
        case FieldTypeId.Boolean => BooleanType
        case FieldTypeId.Enum => StringType
        case FieldTypeId.String => StringType
        case FieldTypeId.Date => DateType
        case FieldTypeId.Json => NullType // TODO
        case FieldTypeId.Null => NullType
      }

      val metadata = if (spec.fieldType == FieldTypeId.Boolean) {
        val aliases = Seq(
          spec.displayTrueValue.map(_ -> "true"),
          spec.displayFalseValue.map(_ -> "false")
        ).flatten

        val jsonMetadata = Json.obj("ml_attr" -> Json.obj(
          "aliases" -> Json.obj(
            "from" -> JsArray(aliases.map(x => JsString(x._1))),
            "to" -> JsArray(aliases.map(x => JsString(x._2)))
          )),
          "type" -> "nominal"
        )
        Metadata.fromJson(Json.stringify(jsonMetadata))
      } else
        Metadata.empty


      val enumLabels = if (spec.fieldType == FieldTypeId.Enum) {
        spec.enumValues.map(_._2).toSeq.sorted
      } else
        Nil

      val structField = StructField(fieldName, sparkFieldType, true, metadata)
      (structField, enumLabels)
    }

    val structTypes = structTypesWithEnumLabels.map(_._1)
    val stringTypesWithEnumLabels = structTypesWithEnumLabels.filter(_._1.dataType.equals(StringType))

    val df = session.createDataFrame(data, StructType(structTypes))

    // index string columns
    val filteredStringTypesWithEnumLabels = stringTypesWithEnumLabels.filter { case (stringType, _) => !stringFieldsNotToIndex.contains(stringType.name) }
    val stringColumnNamesWithEnumLabels = filteredStringTypesWithEnumLabels.map { case (field, enumLabels) => (field.name, enumLabels) }
    val finalDf = indexStringCols(stringColumnNamesWithEnumLabels)(df)

    (finalDf, valueBroadVar)
  }

  private def discretizeAsQuantiles(
    df: DataFrame,
    bucketsNum: Int,
    columnName: String
  ): DataFrame = {
    val outputColumnName = columnName + "_" + Random.nextLong()
    val discretizer = new QuantileDiscretizer()
      .setInputCol(columnName)
      .setOutputCol(outputColumnName)
      .setNumBuckets(bucketsNum)

    val result = discretizer.fit(df).transform(df)

    result.drop(columnName).withColumnRenamed(outputColumnName, columnName)
  }
}