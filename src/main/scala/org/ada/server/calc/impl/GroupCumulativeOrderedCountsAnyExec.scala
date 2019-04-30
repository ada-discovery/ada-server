package org.ada.server.calc.impl

import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import org.ada.server.models.{Field, FieldTypeId}
import org.incal.core.dataaccess.{AsyncReadonlyRepo, Criterion}
import play.api.libs.json.JsObject
import org.ada.server.calc.{CalculatorExecutor, ToFields, With2TupleFields, WithSeqFields}
import org.ada.server.calc.CalculatorHelper._
import org.ada.server.field.FieldUtil.{fieldTypeOrdering, valueOrdering}

import scala.reflect.runtime.universe._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private class GroupCumulativeOrderedCountsAnyExec[G, F](
    implicit inputTypeTag: TypeTag[GroupCumulativeOrderedCountsCalcTypePack[G, Any]#IN]
  ) extends CalculatorExecutor[GroupCumulativeOrderedCountsCalcTypePack[G, Any], F] {

  toFields: ToFields[F] =>

  private def anyExec(
    implicit ordering: Ordering[Any]
  ) = {
    CalculatorExecutor.withSeq(GroupCumulativeOrderedCountsCalc.apply[G, Any])
  }

  override def exec(
    options: Unit)(
    values: Traversable[(Option[G], Option[Any])]
  ) =
    values.find(_._2.isDefined).map { case (_, Some(someVal)) =>
      dispatchVal(
        _.exec(options)(values)
      )(someVal, Nil)
    }.getOrElse(
      Nil
    )

  override def execJson(
    options: Unit,
    fields: F)(
    jsons: Traversable[JsObject]
  ) = dispatch(
      _.execJson(options, toFields(fields))(jsons)
    )(fields, Nil)

  override def execJsonA(
    options: Unit,
    scalarOrArrayField: Field,
    fields: F)(
    jsons: Traversable[JsObject]
  ) = dispatch(
      _.execJsonA(options, scalarOrArrayField, toFields(fields))(jsons)
    )(fields, Nil)

  override def execJsonStreamed(
    flowOptions: Unit,
    postFlowOptions: Unit,
    fields: F)(
    source: Source[JsObject, _])(
    implicit materializer: Materializer
  ) = dispatch(
      _.execJsonStreamed(flowOptions, postFlowOptions, toFields(fields))(source)
    )(fields, Future(Nil))

  override def execJsonStreamedA(
    flowOptions: Unit,
    postFlowOptions: Unit,
    scalarOrArrayField: Field,
    fields: F)(
    source: Source[JsObject, _])(
    implicit materializer: Materializer
  ) = dispatch(
      _.execJsonStreamedA(flowOptions, postFlowOptions, scalarOrArrayField, toFields(fields))(source)
    )(fields, Future(Nil))

  override def createJsonFlow(
    options: Unit,
    fields: F
  ) = dispatch(
      _.createJsonFlow(options, toFields(fields))
    )(fields, Flow[JsObject].map(_ => Nil))

  override def createJsonFlowA(
    options: Unit,
    scalarOrArrayField: Field,
    fields: F
  ) = dispatch(
      _.createJsonFlowA(options, scalarOrArrayField, toFields(fields))
    )(fields, Flow[JsObject].map(_ => Nil))

  override def execJsonRepoStreamed(
    flowOptions: Unit,
    postFlowOptions: Unit,
    withProjection: Boolean,
    fields: F)(
    dataRepo: AsyncReadonlyRepo[JsObject, _],
    criteria: Seq[Criterion[Any]])(
    implicit materializer: Materializer
  ) = dispatch(
      _.execJsonRepoStreamed(flowOptions, postFlowOptions, withProjection, toFields(fields))(dataRepo, criteria)
    )(fields, Future(Nil))

  override def execJsonRepoStreamedA(
    flowOptions: Unit,
    postFlowOptions: Unit,
    withProjection: Boolean,
    scalarOrArrayField: Field,
    fields: F)(
    dataRepo: AsyncReadonlyRepo[JsObject, _],
    criteria: Seq[Criterion[Any]])(
    implicit materializer: Materializer
  ) = dispatch(
      _.execJsonRepoStreamedA(flowOptions, postFlowOptions, withProjection, scalarOrArrayField, toFields(fields))(dataRepo, criteria)
    )(fields, Future(Nil))

  override def execStreamed(
    flowOptions: Unit,
    postFlowOptions: Unit)(
    source: Source[(Option[G], Option[Any]), _])(
    implicit materializer: Materializer
  ) =
    throw new RuntimeException("Method GroupCumulativeOrderedCountsAnyExec.execStreamed is not supported due to unknown value type (ordering).")

  override def execPostFlow(
    options: Unit)(
    flowOutput: Traversable[((Option[G], Any), Int)]
  ) = {
    flowOutput.headOption.map { case ((_, someVal), _) =>
      dispatchVal(
        _.execPostFlow(options)(flowOutput)
      )(someVal, Nil)
    }.getOrElse(
      Nil
    )
  }

  // helper / dispatch functions

  private def dispatch[OUT](
    exec: CalculatorExecutor[GroupCumulativeOrderedCountsCalcTypePack[G, Any], Seq[Field]] => OUT)(
    fields: F,
    defaultOutput: OUT
  ): OUT =
    // assume the second field is the one determining a type
    toFields(fields).tail.headOption.flatMap( field =>
      fieldTypeOrdering(field.fieldType).map { implicit ordering =>
        exec(anyExec)
      }
    ).getOrElse(defaultOutput)

  private def dispatchVal[OUT](
    exec: CalculatorExecutor[GroupCumulativeOrderedCountsCalcTypePack[G, Any], Seq[Field]] => OUT)(
    value: Any,
    defaultOutput: OUT
  ): OUT =
    valueOrdering(value).map { implicit ordering =>
      exec(anyExec)
    }.getOrElse(defaultOutput)
}

object GroupCumulativeOrderedCountsAnyExec {

  def with2Tuple[G](
    implicit inputTypeTag: TypeTag[GroupCumulativeOrderedCountsCalcTypePack[G, Any]#IN]
  ): CalculatorExecutor[GroupCumulativeOrderedCountsCalcTypePack[G, Any], (Field, Field)] = new GroupCumulativeOrderedCountsAnyExec[G, (Field, Field)] with With2TupleFields

  def withSeq[G](
    implicit inputTypeTag: TypeTag[GroupCumulativeOrderedCountsCalcTypePack[G, Any]#IN]
  ): CalculatorExecutor[GroupCumulativeOrderedCountsCalcTypePack[G, Any], Seq[Field]] = new GroupCumulativeOrderedCountsAnyExec[G, Seq[Field]] with WithSeqFields
}