package org.ada.server.util

import java.lang.reflect.Modifier
import org.reflections.Reflections

import scala.reflect.ClassTag
import scala.collection.JavaConversions.asScalaSet

object ClassFinderUtil {

  def findClasses[T](
    packageName: Option[String],
    exactPackageMatch: Boolean = false)(
    implicit m: ClassTag[T]
  ): Traversable[Class[T]] = {
    val reflections = packageName.map(new Reflections(_)).getOrElse(new Reflections())
    val clazz = m.runtimeClass

    val classes = reflections.getSubTypesOf(clazz).filter(currentClazz =>
      !currentClazz.isPrimitive && !Modifier.isAbstract(currentClazz.getModifiers) && !currentClazz.isInterface
    ).map(_.asInstanceOf[Class[T]])

    if (exactPackageMatch && packageName.isDefined) {
      classes.filter(_.getPackage.getName.equals(packageName.get))
    } else
      classes
  }
}