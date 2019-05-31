package org.ada.server.services

import org.ada.server.util.ClassFinderUtil.findClasses
import org.incal.core.util.ReflectionUtil.{currentThreadClassLoader, newMirror, staticInstance}

import scala.reflect.ClassTag

trait StaticLookupCentral[T] extends (() => Traversable[T])

class StaticLookupCentralImpl[T: ClassTag](
  lookupPackageName: String,
  exactPackageMatch: Boolean = true
) extends StaticLookupCentral[T]{

  def apply = {
    val currentMirror = newMirror(currentThreadClassLoader)

    findClasses[T](Some(lookupPackageName), exactPackageMatch).flatMap { importerClazz =>
      try {
        Some(staticInstance(importerClazz.getName, currentMirror).asInstanceOf[T])
      } catch {
        case _: ScalaReflectionException => None
      }
    }
  }
}
