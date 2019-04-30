package org.ada.server.calc.impl

import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import org.ada.server.models.{Field, FieldTypeId}
import org.incal.core.dataaccess.{AsyncReadonlyRepo, Criterion}
import play.api.libs.json.JsObject
import org.ada.server.calc._
import org.ada.server.field.FieldUtil.{fieldTypeOrdering, valueOrdering}

import scala.reflect.runtime.universe._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait GroupQuartilesCalcNoOptionsTypePack[G, T] extends NoOptionsCalculatorTypePack {
  type IN = (Option[G], Option[T])
  type OUT = Traversable[(Option[G], Option[Quartiles[T]])]
  type INTER = Traversable[(Option[G], Traversable[T])]
}

trait GroupQuartilesCalcTypePackAux[G] {
  type Type[T] = GroupQuartilesCalcTypePack[G, T]
}

private class GroupQuartilesAnyExec[G: TypeTag, F] extends CalculatorExecutor[GroupQuartilesCalcNoOptionsTypePack[G, Any], F] with DispatchHelper[F, GroupQuartilesCalcTypePackAux[G]#Type] {

  toFields: ToFields[F] =>

  override protected def genericExec[T: Ordering](
    implicit inputTypeTag: TypeTag[T]
  ) = CalculatorExecutor.withSeq(GroupQuartilesCalc[G, T])

  override def exec(
    options: Unit)(
    values: Traversable[(Option[G], Option[Any])]
  ) =
    values.find(_._2.isDefined).map { case (_, Some(someVal)) =>
      dispatchVal(
        (executor) => executor.exec(_)(values)
      )(someVal, None)
    }.getOrElse(Nil)

  override def execJson(
    options: Unit,
    fields: F)(
    jsons: Traversable[JsObject]
  ) = dispatch(
      (executor) => executor.execJson(_, toFields(fields))(jsons)
    )(fields, None)

  override def execJsonA(
    options: Unit,
    scalarOrArrayField: Field,
    fields: F)(
    jsons: Traversable[JsObject]
  ) = dispatch(
      (executor) => executor.execJsonA(_, scalarOrArrayField, toFields(fields))(jsons)
    )(fields, None)

  override def execJsonStreamed(
    flowOptions: Unit,
    postFlowOptions: Unit,
    fields: F)(
    source: Source[JsObject, _])(
    implicit materializer: Materializer
  ) = dispatch(
      (executor) => executor.execJsonStreamed(flowOptions, _, toFields(fields))(source)
    )(fields, Future(None))

  override def execJsonStreamedA(
    flowOptions: Unit,
    postFlowOptions: Unit,
    scalarOrArrayField: Field,
    fields: F)(
    source: Source[JsObject, _])(
    implicit materializer: Materializer
  ) = dispatch(
      (executor) => executor.execJsonStreamedA(flowOptions, _, scalarOrArrayField, toFields(fields))(source)
    )(fields, Future(None))

  override def createJsonFlow(
    options: Unit,
    fields: F
  ) = dispatchPlain(
      _.createJsonFlow(options, toFields(fields))
    )(fields, Flow[JsObject].map(_ => Nil))

  override def createJsonFlowA(
    options: Unit,
    scalarOrArrayField: Field,
    fields: F
  ) = dispatchPlain(
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
      (executor) => executor.execJsonRepoStreamed(flowOptions, _, withProjection, toFields(fields))(dataRepo, criteria)
    )(fields, Future(None))

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
      (executor) => executor.execJsonRepoStreamedA(flowOptions, _, withProjection, scalarOrArrayField, toFields(fields))(dataRepo, criteria)
    )(fields, Future(None))

  override def execStreamed(
    flowOptions: Unit,
    postFlowOptions: Unit)(
    source: Source[(Option[G], Option[Any]), _])(
    implicit materializer: Materializer
  ) =
    throw new RuntimeException("Method GroupQuartilesCalcNoOptionsTypePack.execStreamed is not supported due to unknown value type (ordering).")

  override def execPostFlow(
    options: Unit)(
    flowOutput: Traversable[(Option[G], Traversable[Any])]
  ) = {
    val value = flowOutput.flatMap(_._2).headOption

    value.map { someVal =>
      dispatchVal(
        (executor) => executor.execPostFlow(_)(flowOutput)
      )(someVal, None)
    }.getOrElse(Nil)
  }
}

object GroupQuartilesAnyExec {
  def withSingle[G: TypeTag]: CalculatorExecutor[GroupQuartilesCalcNoOptionsTypePack[G, Any], Field] =
    new GroupQuartilesAnyExec[G, Field] with WithSingleField

  def withSeq[G: TypeTag]: CalculatorExecutor[GroupQuartilesCalcNoOptionsTypePack[G, Any], Seq[Field]] =
    new GroupQuartilesAnyExec[G, Seq[Field]] with WithSeqFields
}