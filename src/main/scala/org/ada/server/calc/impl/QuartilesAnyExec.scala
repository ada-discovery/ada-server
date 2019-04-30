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

trait QuartilesCalcNoOptionsTypePack[T] extends NoOptionsCalculatorTypePack {
  type IN = Option[T]
  type OUT = Option[Quartiles[T]]
  type INTER = Traversable[T]
}

private trait QuartilesAnyExec[F] extends CalculatorExecutor[QuartilesCalcNoOptionsTypePack[Any], F] with DispatchHelper[F, QuartilesCalcTypePack] {

  toFields: ToFields[F] =>

  override protected def genericExec[T: Ordering](
    implicit inputTypeTag: TypeTag[T]
  ) = CalculatorExecutor.withSeq(QuartilesCalc[T])

  override def exec(
    options: Unit)(
    values: Traversable[Option[Any]]
  ) =
    values.find(_.isDefined).flatMap { case Some(someVal) =>
      dispatchVal(
        (executor) => executor.exec(_)(values)
      )(someVal, None)
    }

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
    source: Source[Option[Any], _])(
    implicit materializer: Materializer
  ) =
    throw new RuntimeException("Method QuartilesAnyExec.execStreamed is not supported due to unknown value type (ordering).")

  override def execPostFlow(
    options: Unit)(
    flowOutput: Traversable[Any]
  ) =
    flowOutput.headOption.flatMap { someVal =>
      dispatchVal(
        (executor) => executor.execPostFlow(_)(flowOutput)
      )(someVal, None)
    }
}

trait DispatchHelper[F, C[X] <: CalculatorTypePack] {

  toFields: ToFields[F] =>

  protected def valueField(fields: F): Option[Field] = {
    val fieldz = toFields(fields)
    fieldz.headOption.map(_ => fieldz.last)
  }

  protected def genericExec[T: Ordering](
    implicit inputTypeTag: TypeTag[T]
  ): CalculatorExecutor[C[T], Seq[Field]] with WithSeqFields

  // auxiliary functions
  protected def dispatch[OUT](
    exec: CalculatorExecutor[C[Any], Seq[Field]] => ((Any => Double) => OUT))(
    fields: F,
    defaultOutput: OUT
  ): OUT =
    valueField(fields).flatMap( field =>
      fieldTypeToDouble(field.fieldType).flatMap ( toDouble =>
        fieldTypeOrdering(field.fieldType).map { implicit ordering =>
          exec(genericExec[Any])(toDouble)
        }
      )
    ).getOrElse(defaultOutput)

  protected def dispatchPlain[OUT](
    exec: CalculatorExecutor[C[Any], Seq[Field]] => OUT)(
    fields: F,
    defaultOutput: OUT
  ): OUT =
    valueField(fields).flatMap( field =>
      fieldTypeOrdering(field.fieldType).map { implicit ordering =>
        exec(genericExec[Any])
      }
    ).getOrElse(defaultOutput)

  protected def dispatchVal[OUT](
    exec: CalculatorExecutor[C[Any], Seq[Field]] => ((Any => Double) => OUT))(
    value: Any,
    defaultOutput: OUT
  ): OUT =
    valueToDouble(value).flatMap ( toDouble =>
      valueOrdering(value).map { implicit ordering =>
        exec(genericExec[Any])(toDouble)
      }
    ).getOrElse(defaultOutput)

  private def fieldTypeToDouble(
    fieldTypeId: FieldTypeId.Value
  ): Option[Any => Double] = {
    def aux[T](toDouble: T => Double) =
      Some(toDouble.asInstanceOf[Any => Double])

    fieldTypeId match {
      case FieldTypeId.Enum => aux[Int](_.toDouble)
      case FieldTypeId.Boolean => aux[Boolean](if(_) 1d else 0d)
      case FieldTypeId.Double => aux[Double](identity)
      case FieldTypeId.Integer => aux[Long](_.toDouble)
      case FieldTypeId.Date => aux[java.util.Date](_.getTime.toDouble)
      case _ => None
    }
  }

  private def valueToDouble(
    value: Any
  ): Option[Any => Double] = {
    def aux[T](toDouble: T => Double) =
      Some(toDouble.asInstanceOf[Any => Double])

    value match {
      case _: Boolean => aux[Boolean](if(_) 1d else 0d)
      case _: Double => aux[Double](identity)
      case _: Float => aux[Float](_.toDouble)
      case _: Long => aux[Long](_.toDouble)
      case _: Int => aux[Int](_.toDouble)
      case _: Short => aux[Short](_.toDouble)
      case _: Byte => aux[Byte](_.toDouble)
      case _: java.util.Date => aux[java.util.Date](_.getTime.toDouble)
      case _ => None
    }
  }
}

object QuartilesAnyExec {
  def withSingle: CalculatorExecutor[QuartilesCalcNoOptionsTypePack[Any], Field] =
    new QuartilesAnyExec[Field] with WithSingleField

  def withSeq: CalculatorExecutor[QuartilesCalcNoOptionsTypePack[Any], Seq[Field]] =
    new QuartilesAnyExec[Seq[Field]] with WithSeqFields
}