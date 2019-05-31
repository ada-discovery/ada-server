package org.ada.server.services

import org.ada.server.AdaException
import org.ada.server.util.ClassFinderUtil.findClasses
import org.incal.core.runnables.InputFutureRunnable
import org.incal.core.util.ReflectionUtil.classNameToRuntimeType
import play.api.inject.Injector

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

trait LookupCentralExec[IN] extends (IN => Future[Unit])

abstract protected[services] class LookupCentralExecImpl[IN, E <: InputFutureRunnable[IN] : ClassTag](
  lookupPackage: String,
  execName: String
) extends LookupCentralExec[IN] {

  protected val injector: Injector

  private val inputInstanceMap =
    findClasses[E](Some(lookupPackage), true).map { execClazz =>
      val instance = injector.instanceOf(execClazz)
      (instance.inputType -> instance)
    }

  override def apply(input: IN): Future[Unit] = {
    val inputType = classNameToRuntimeType(input.getClass.getName)

    val (_, executor) = inputInstanceMap.find { case (execInputType, _) => execInputType =:= inputType }.getOrElse(
      throw new AdaException(s"No $execName found for the input type ${inputType.typeSymbol.fullName}.")
    )

    for {
      _ <- executor.runAsFuture(input)
      _ <- postExec(input, executor)
    } yield ()
  }

  protected def postExec(input: IN, exec: E): Future[Unit] = Future(())
}