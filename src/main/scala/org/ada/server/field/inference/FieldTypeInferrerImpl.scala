package org.ada.server.field.inference

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.ada.server.AdaException
import org.ada.server.akka.AkkaStreamUtil
import org.ada.server.field.{FieldType, FieldTypeFactory}
import org.ada.server.models.{FieldTypeId, FieldTypeSpec}
import play.api.libs.json.JsReadable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private trait FieldTypeInferrerImpl[T] extends FieldTypeInferrer[T] {

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

  protected def dynamicFieldInferrers: Seq[((FieldTypeId.Value, Boolean), SingleFieldTypeInferrer.of[T])]

  protected def createStaticChecker(fieldType: FieldType[_]): StaticFieldTypeInferrer.of[T]

  private val staticFieldInferrers =
    staticFieldTypes.map(fieldType =>
      ((fieldType.spec.fieldType, fieldType.spec.isArray), createStaticChecker(fieldType))
    )

  private val semiStaticFieldInferrers =
    semiStaticFieldTypeSpecs.map(fieldTypeSpec =>
      ((fieldTypeSpec.fieldType, fieldTypeSpec.isArray), createStaticChecker(ftf(fieldTypeSpec)))
    )

  private lazy val fieldTypeInferrers: Traversable[((FieldTypeId.Value, Boolean), SingleFieldTypeInferrer.of[T])] =
    staticFieldInferrers ++
      semiStaticFieldInferrers ++
        dynamicFieldInferrers

  private lazy val fieldTypeInferrerMap = fieldTypeInferrers.toMap

  override def apply(values: Traversable[T]): FieldType[_] = {
    val fieldTypes = prioritizedFieldTypes.view.map( fieldTypeSpec =>
      fieldTypeInferrerMap.get(fieldTypeSpec).flatMap(_.fun()(values))
    )

    selectFirst(fieldTypes)
  }

  override def apply(
    source: Source[T, _])(
    implicit materializer: Materializer
  ): Future[FieldType[_]] = {
    // inferrers
    val orderedInferrers = prioritizedFieldTypes.map { fieldTypeSpec =>
      fieldTypeInferrerMap.get(fieldTypeSpec).getOrElse(throw new AdaException(s"Field type ${fieldTypeSpec} not recognized."))
    }

    // collect all the flows
    val flows: Seq[Flow[T, Any, NotUsed]] = orderedInferrers.map(_.flow())

    // collect all the post flows
    val postFlows: Seq[Any => Option[FieldType[_]]] = orderedInferrers.map(_.postFlow().asInstanceOf[Any => Option[FieldType[_]]])

    // zip the flows
    val zippedFlow = AkkaStreamUtil.zipNFlows(flows)

    for {
      flowOutputs <- source.via(zippedFlow).runWith(Sink.head)
    } yield {
      val fieldTypes = flowOutputs.zip(postFlows).par.map { case (flowOutput, postFlow) =>
        postFlow(flowOutput)
      }.toList

      selectFirst(fieldTypes)
    }
  }

  private def selectFirst(orderedFieldTypes: Seq[Option[FieldType[_]]]) = {
    // select the first one that is defined
    val fieldType = orderedFieldTypes.flatten.headOption

    fieldType match {
      case Some(fieldType) => fieldType
      // this should never happen
      case None => defaultType
    }
  }
}

private final class DisplayStringFieldTypeInferrerImpl(
  val ftf: FieldTypeFactory,
  val maxEnumValuesCount: Int,
  val minAvgValuesPerEnum: Double,
  val arrayDelimiter: String
) extends FieldTypeInferrerImpl[String] {

  override protected val dynamicFieldInferrers: Seq[((FieldTypeId.Value, Boolean), SingleFieldTypeInferrer.of[String])] = Seq(
    (
      (FieldTypeId.Enum, false),
      EnumFieldTypeInferrer.ofString(stringType, maxEnumValuesCount, minAvgValuesPerEnum)
    ),
    (
      (FieldTypeId.Enum, true),
      EnumFieldTypeInferrer.ofStringArray(stringArrayType, maxEnumValuesCount, minAvgValuesPerEnum, arrayDelimiter)
    )
  )

  override protected def createStaticChecker(fieldType: FieldType[_]) =
    StaticFieldTypeInferrer.ofString(fieldType)
}

private final class DisplayJsonFieldTypeInferrerImpl(
  val ftf: FieldTypeFactory,
  val maxEnumValuesCount: Int,
  val minAvgValuesPerEnum: Double,
  val arrayDelimiter: String
) extends FieldTypeInferrerImpl[JsReadable] {

  override protected val dynamicFieldInferrers: Seq[((FieldTypeId.Value, Boolean), SingleFieldTypeInferrer.of[JsReadable])] = Seq(
    (
      (FieldTypeId.Enum, false),
      EnumFieldTypeInferrer.ofJson(stringType, maxEnumValuesCount, minAvgValuesPerEnum)
    ),
    (
      (FieldTypeId.Enum, true),
      EnumFieldTypeInferrer.ofJsonArray(stringArrayType, maxEnumValuesCount, minAvgValuesPerEnum, arrayDelimiter)
    )
  )

  override protected def createStaticChecker(fieldType: FieldType[_]) =
    StaticFieldTypeInferrer.ofJson(fieldType)
}

