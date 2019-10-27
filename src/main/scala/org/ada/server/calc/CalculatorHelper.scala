package org.ada.server.calc

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.ada.server.models.Field
import org.incal.core.dataaccess.{AsyncReadonlyRepo, Criterion}
import play.api.libs.json.JsObject
import org.ada.server.calc.impl.{ArrayCalc, ArrayCalculatorTypePack}
import org.ada.server.calc.json.JsonInputConverterFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.universe._

object CalculatorHelper {

  private val libPath = "lib"
  private val jicf = JsonInputConverterFactory

  implicit class RunExt[C <: CalculatorTypePack] (
    val calculator: Calculator[C])(
    implicit materializer: Materializer
  ) {
    def runFlow(
      options2: C#FLOW_OPT,
      options3: C#SINK_OPT)(
      source: Source[C#IN, _]
    ): Future[C#OUT] =
      source.via(calculator.flow(options2)).runWith(Sink.head).map(calculator.postFlow(options3))
  }

  implicit class JsonExt[C <: CalculatorTypePack](
    val calculator: Calculator[C])(
    implicit inputTypeTag: TypeTag[C#IN]
  ) {

    def jsonFun(
      fields: Seq[Field],
      options: C#OPT
    ): Traversable[JsObject] => C#OUT = {
      (jsons: Traversable[JsObject]) =>
        val jsonConverter = jsonConvert(fields)
        val inputs = jsons.map(jsonConverter)
        calculator.fun(options)(inputs)
    }

    def jsonFlow(
      fields: Seq[Field],
      options: C#FLOW_OPT
    ): Flow[JsObject, C#INTER, NotUsed] = {
      val jsonConverter = jsonConvert(fields)
      Flow[JsObject].map(jsonConverter).via(calculator.flow(options))
    }

    def runJsonFlow(
      fields: Seq[Field],
      options2: C#FLOW_OPT,
      options3: C#SINK_OPT)(
      source: Source[JsObject, _])(
      implicit materializer: Materializer
    ): Future[C#OUT] =
      source.via(jsonFlow(fields, options2)).runWith(Sink.head).map(calculator.postFlow(options3))

    private def jsonConvert(fields: Seq[Field]) = {
      val unwrappedCalculator =
        calculator match {
          case arrayCalc: ArrayCalc[C, _] => arrayCalc.innerCalculator
          case _ => calculator
        }
      jicf.apply[C#IN](unwrappedCalculator).apply(fields)
    }
  }

  implicit class ArrayJsonExt[C <: CalculatorTypePack](
    val calculator: Calculator[C])(
    implicit inputTypeTag: TypeTag[C#IN], arrayTypeTag: TypeTag[ArrayCalculatorTypePack[C]#IN]
  ) {

    /**
      * Transforms jsons to the expected input and executes.
      * If the provided <code>scalarOrArrayField</code> is array, jsons are transformed to a sequence of arrays, otherwise plain scalars
      *
      * @param scalarOrArrayField
      * @param fields
      * @param options
      * @return
      */
    def jsonFunA(
      scalarOrArrayField: Field,
      fields: Seq[Field],
      options: C#OPT
    ): Traversable[JsObject] => C#OUT =
      if (scalarOrArrayField.isArray) {
        ArrayCalc(calculator).jsonFun(fields, options)
      } else
        calculator.jsonFun(fields, options)

    /**
      * Creates a flow (stream) for an execution with inputs transformed from provided jsons.
      * If the provided <code>scalarOrArrayField</code> is array, each json is transformed to a array, otherwise a plain scalar
      *
      * @param scalarOrArrayField
      * @param fields
      * @param options
      * @return
      */
    def jsonFlowA(
      scalarOrArrayField: Field,
      fields: Seq[Field],
      options: C#FLOW_OPT
    ): Flow[JsObject, C#INTER, NotUsed] =
      if (scalarOrArrayField.isArray)
        ArrayCalc(calculator).jsonFlow(fields, options)
      else
        calculator.jsonFlow(fields, options)

    def runJsonFlowA(
      scalarOrArrayField: Field,
      fields: Seq[Field],
      options2: C#FLOW_OPT,
      options3: C#SINK_OPT)(
      source: Source[JsObject, _])(
      implicit materializer: Materializer
    ): Future[C#OUT] =
      if (scalarOrArrayField.isArray)
        ArrayCalc(calculator).runJsonFlow(fields, options2, options3)(source)
      else
        calculator.runJsonFlow(fields, options2, options3)(source)
  }

  implicit class NoOptionsExt[C <: NoOptionsCalculatorTypePack](
    val calculator: Calculator[C]
  ) {
    def fun_ = calculator.fun(())

    def flow_ = calculator.flow(())

    def postFlow_ = calculator.postFlow(())

    def runFlow_(
      source: Source[C#IN, _])(
      implicit materializer: Materializer
    ) = calculator.runFlow((), ())(source)
  }

  implicit class NoOptionsJsonExt[C <: NoOptionsCalculatorTypePack](
    val calculator: Calculator[C])(
    implicit typeTag: TypeTag[C#IN]
  ) {
    def jsonFun_(fields: Field*) =
      calculator.jsonFun(fields, ())

    def jsonFunA_(scalarOrArrayField: Field, fields: Field*) =
      calculator.jsonFunA(scalarOrArrayField, fields, ())

    def jsonFlow_(fields: Field*) =
      calculator.jsonFlow(fields, ())

    def jsonFlowA_(scalarOrArrayField: Field, fields: Field*) =
      calculator.jsonFlowA(scalarOrArrayField, fields, ())

    def runJsonFlow_(
      fields: Field*)(
      source: Source[JsObject, _])(
      implicit materializer: Materializer
    ) =
      calculator.runJsonFlow(fields, (), ())(source)

    def runJsonFlowA_(
      scalarOrArrayField: Field,
      fields: Field*)(
      source: Source[JsObject, _])(
      implicit materializer: Materializer
    ) =
      calculator.runJsonFlowA(scalarOrArrayField, fields, (), ())(source)
  }

  implicit class NoOptionsExecExt[C <: NoOptionsCalculatorTypePack, F](
    val calculatorExecutor: CalculatorExecutor[C, F]
  ) {
    def exec_ =
      calculatorExecutor.exec(())(_)

    def execStreamed_(
      source: Source[C#IN, _])(
      implicit materializer: Materializer
    ) = calculatorExecutor.execStreamed((), ())(source)

    def execJson_(
      fields: F
    ) = calculatorExecutor.execJson((), fields)(_)

    def execJsonA_(
      scalarOrArrayField: Field,
      fields: F
    ) = calculatorExecutor.execJsonA((), scalarOrArrayField, fields)(_)

    def execJsonStreamed_(
      fields: F)(
      source: Source[JsObject, _])(
      implicit materializer: Materializer
    ) = calculatorExecutor.execJsonStreamed((), (), fields)(source)

    def execJsonStreamedA_(
      scalarOrArrayField: Field,
      fields: F)(
      source: Source[JsObject, _])(
      implicit materializer: Materializer
    ) = calculatorExecutor.execJsonStreamedA((), (), scalarOrArrayField, fields)(source)

    def execJsonRepoStreamed_(
      withProjection: Boolean,
      fields: F)(
      dataRepo: AsyncReadonlyRepo[JsObject, _],
      criteria: Seq[Criterion[Any]])(
      implicit materializer: Materializer
    ) = calculatorExecutor.execJsonRepoStreamed((), (), withProjection, fields)(dataRepo, criteria)

    def execJsonRepoStreamedA_(
      withProjection: Boolean,
      scalarOrArrayField: Field,
      fields: F)(
      dataRepo: AsyncReadonlyRepo[JsObject, _],
      criteria: Seq[Criterion[Any]])(
      implicit materializer: Materializer
    ) = calculatorExecutor.execJsonRepoStreamedA((), (), withProjection, scalarOrArrayField, fields)(dataRepo, criteria)

    def createJsonFlow_(
      fields: F
    ) = calculatorExecutor.createJsonFlow((), fields)

    def createJsonFlowA_(
      scalarOrArrayField: Field,
      fields: F
    ) = calculatorExecutor.createJsonFlowA((), scalarOrArrayField, fields)

    def execPostFlow_ =
      calculatorExecutor.execPostFlow(())(_)
  }

  implicit class SingleFieldExecExt[C <: CalculatorTypePack](
    val calculatorExecutor: CalculatorExecutor[C, Field]
  ) {
    def execJsonA_(
      options: C#OPT,
      field: Field
    ) = calculatorExecutor.execJsonA(options, field, field)(_)

    def execJsonStreamedA_(
      flowOptions: C#FLOW_OPT,
      postFlowOptions: C#SINK_OPT,
      field: Field)(
      source: Source[JsObject, _])(
      implicit materializer: Materializer
    ) = calculatorExecutor.execJsonStreamedA(flowOptions, postFlowOptions, field, field)(source)


    def execJsonRepoStreamedA_(
      flowOptions: C#FLOW_OPT,
      postFlowOptions: C#SINK_OPT,
      withProjection: Boolean,
      field: Field)(
      dataRepo: AsyncReadonlyRepo[JsObject, _],
      criteria: Seq[Criterion[Any]])(
      implicit materializer: Materializer
    ) = calculatorExecutor.execJsonRepoStreamedA(flowOptions, postFlowOptions, withProjection, field, field)(dataRepo, criteria)

    def createJsonFlowA_(
      options: C#FLOW_OPT,
      field: Field
    ) = calculatorExecutor.createJsonFlowA(options, field, field)
  }
}

trait FullDataCalculatorTypePack extends CalculatorTypePack {
  override type INTER = Traversable[IN]
  override type FLOW_OPT = Unit
  override type SINK_OPT = OPT
}

trait NoOptionsCalculatorTypePack extends CalculatorTypePack {
  override type OPT = Unit
  override type FLOW_OPT = Unit
  override type SINK_OPT = Unit
}