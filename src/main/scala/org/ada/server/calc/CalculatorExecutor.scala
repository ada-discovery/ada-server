package org.ada.server.calc

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import org.ada.server.models.Field
import org.incal.core.dataaccess.{AsyncReadonlyRepo, Criterion}
import play.api.libs.json.JsObject
import reactivemongo.bson.BSONObjectID
import org.ada.server.calc.CalculatorHelper._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.runtime.universe._
import scala.concurrent.Future

trait CalculatorExecutor[C <: CalculatorTypePack, F] {

  // functions
  def exec(
    options: C#OPT)(
    values: Traversable[C#IN]
  ): C#OUT

  def execStreamed(
    flowOptions: C#FLOW_OPT,
    postFlowOptions: C#SINK_OPT)(
    source: Source[C#IN, _])(
    implicit materializer: Materializer
  ): Future[C#OUT]

  def execJson(
    options: C#OPT,
    fields: F)(
    jsons: Traversable[JsObject]
  ): C#OUT

  def execJsonA(
    options: C#OPT,
    scalarOrArrayField: Field,
    fields: F)(
    jsons: Traversable[JsObject]
  ): C#OUT

  def execJsonStreamed(
    flowOptions: C#FLOW_OPT,
    postFlowOptions: C#SINK_OPT,
    fields: F)(
    source: Source[JsObject, _])(
    implicit materializer: Materializer
  ): Future[C#OUT]

  def execJsonStreamedA(
    flowOptions: C#FLOW_OPT,
    postFlowOptions: C#SINK_OPT,
    scalarOrArrayField: Field,
    fields: F)(
    source: Source[JsObject, _])(
    implicit materializer: Materializer
  ): Future[C#OUT]

  def execJsonRepoStreamed(
    flowOptions: C#FLOW_OPT,
    postFlowOptions: C#SINK_OPT,
    withProjection: Boolean,
    fields: F)(
    dataRepo: AsyncReadonlyRepo[JsObject, _],
    criteria: Seq[Criterion[Any]])(
    implicit materializer: Materializer
  ): Future[C#OUT]

  def execJsonRepoStreamedA(
    flowOptions: C#FLOW_OPT,
    postFlowOptions: C#SINK_OPT,
    withProjection: Boolean,
    scalarOrArrayField: Field,
    fields: F)(
    dataRepo: AsyncReadonlyRepo[JsObject, _],
    criteria: Seq[Criterion[Any]])(
    implicit materializer: Materializer
  ): Future[C#OUT]

  def createJsonFlow(
    options: C#FLOW_OPT,
    fields: F
  ): Flow[JsObject, C#INTER, NotUsed]

  def createJsonFlowA(
    options: C#FLOW_OPT,
    scalarOrArrayField: Field,
    fields: F
  ): Flow[JsObject, C#INTER, NotUsed]

  def execPostFlow(
    options: C#SINK_OPT)(
    flowOutput: C#INTER
  ): C#OUT
}

private class CalculatorExecutorImpl[C <: CalculatorTypePack, F](
  calculator: Calculator[C])(
  implicit inputTypeTag: TypeTag[C#IN]
  ) extends CalculatorExecutor[C, F] {

  toFields: ToFields[F] =>

  override def exec(
    options: C#OPT)(
    values: Traversable[C#IN]
  ): C#OUT = calculator.fun(options)(values)

  override def execStreamed(
    flowOptions: C#FLOW_OPT,
    postFlowOptions: C#SINK_OPT)(
    source: Source[C#IN, _])(
    implicit materializer: Materializer
  ): Future[C#OUT] =
    calculator.runFlow(flowOptions, postFlowOptions)(source)

  override def execJson(
    options: C#OPT,
    fields: F)(
    jsons: Traversable[JsObject]
  ): C#OUT =
    calculator.jsonFun(toFields(fields), options)(jsons)

  override def execJsonA(
    options: C#OPT,
    scalarOrArrayField: Field,
    fields: F)(
    jsons: Traversable[JsObject]
  ): C#OUT =
    calculator.jsonFunA(scalarOrArrayField, toFields(fields), options)(jsons)

  override def execJsonStreamed(
    flowOptions: C#FLOW_OPT,
    postFlowOptions: C#SINK_OPT,
    fields: F)(
    source: Source[JsObject, _])(
    implicit materializer: Materializer
  ): Future[C#OUT] =
    calculator.runJsonFlow(toFields(fields), flowOptions, postFlowOptions)(source)

  override def execJsonStreamedA(
    flowOptions: C#FLOW_OPT,
    postFlowOptions: C#SINK_OPT,
    scalarOrArrayField: Field,
    fields: F)(
    source: Source[JsObject, _])(
    implicit materializer: Materializer
  ): Future[C#OUT] =
    calculator.runJsonFlowA(scalarOrArrayField, toFields(fields), flowOptions, postFlowOptions)(source)

  override def execJsonRepoStreamed(
    flowOptions: C#FLOW_OPT,
    postFlowOptions: C#SINK_OPT,
    withProjection: Boolean,
    fields: F)(
    dataRepo: AsyncReadonlyRepo[JsObject, _],
    criteria: Seq[Criterion[Any]])(
    implicit materializer: Materializer
  ): Future[C#OUT] =
    for {
      // create a data source
      source <- dataRepo.findAsStream(
        criteria = criteria,
        projection = if (withProjection) toFields(fields).map(_.name) else Nil
      )

      // execute the calculator on the data source
      results <- execJsonStreamed(flowOptions, postFlowOptions, fields)(source)
    } yield
      results

  override def execJsonRepoStreamedA(
    flowOptions: C#FLOW_OPT,
    postFlowOptions: C#SINK_OPT,
    withProjection: Boolean,
    scalarOrArrayField: Field,
    fields: F)(
    dataRepo: AsyncReadonlyRepo[JsObject, _],
    criteria: Seq[Criterion[Any]])(
    implicit materializer: Materializer
  ): Future[C#OUT] =
    for {
    // create a data source
      source <- dataRepo.findAsStream(
        criteria = criteria,
        projection = if (withProjection) toFields(fields).map(_.name) else Nil
      )

      // execute the calculator on the data source
      results <- execJsonStreamedA(flowOptions, postFlowOptions, scalarOrArrayField, fields)(source)
    } yield
      results

  override def createJsonFlow(
    flowOptions: C#FLOW_OPT,
    fields: F
  ): Flow[JsObject, C#INTER, NotUsed] =
    calculator.jsonFlow(toFields(fields), flowOptions)

  override def createJsonFlowA(
    flowOptions: C#FLOW_OPT,
    scalarOrArrayField: Field,
    fields: F
  ): Flow[JsObject, C#INTER, NotUsed] =
    calculator.jsonFlowA(scalarOrArrayField, toFields(fields), flowOptions)

  override def execPostFlow(
    options: C#SINK_OPT)(
    flowOutput: C#INTER
   ): C#OUT =
    calculator.postFlow(options)(flowOutput)
}

trait ToFields[F] extends (F => Seq[Field])

trait WithSingleField extends ToFields[Field] {

  override def apply(field: Field) = Seq(field)
}

trait With2TupleFields extends ToFields[(Field, Field)] {

  override def apply(fields: (Field, Field)) = Seq(fields._1, fields._2)
}

trait With3TupleFields extends ToFields[(Field, Field, Field)] {

  override def apply(fields: (Field, Field, Field)) = Seq(fields._1, fields._2, fields._3)
}

trait WithSeqFields extends ToFields[Seq[Field]] {

  override def apply(fields: Seq[Field]) = fields
}

object CalculatorExecutor {

  def withSingle[C <: CalculatorTypePack](
    calculator: Calculator[C])(
    implicit inputTypeTag: TypeTag[C#IN]
  ): CalculatorExecutor[C, Field] =
    new CalculatorExecutorImpl[C, Field](calculator) with WithSingleField

  def with2Tuple[C <: CalculatorTypePack](
    calculator: Calculator[C])(
    implicit inputTypeTag: TypeTag[C#IN]
  ): CalculatorExecutor[C, (Field, Field)] = new CalculatorExecutorImpl[C, (Field, Field)](calculator) with With2TupleFields

  def with3Tuple[C <: CalculatorTypePack](
    calculator: Calculator[C])(
    implicit inputTypeTag: TypeTag[C#IN]
  ): CalculatorExecutor[C, (Field, Field, Field)] = new CalculatorExecutorImpl[C, ((Field, Field, Field))](calculator) with With3TupleFields

  def withSeq[C <: CalculatorTypePack](
    calculator: Calculator[C])(
    implicit inputTypeTag: TypeTag[C#IN]
  ): CalculatorExecutor[C, Seq[Field]] with WithSeqFields = new CalculatorExecutorImpl[C, Seq[Field]](calculator) with WithSeqFields
}