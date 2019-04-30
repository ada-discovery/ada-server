package org.ada.server.util

import java.io.File
import java.lang.reflect.Modifier

import org.ada.server.calc.json.JsonInputConverter
import org.clapper.classutil.{ClassFinder, ClassInfo}
import play.api.Logger
import org.reflections.Reflections

import scala.reflect.ClassTag
import scala.collection.JavaConversions.asScalaSet

object ClassFinderUtil {

//  private val tomcatLibFolder = "../webapps/ROOT/WEB-INF/lib/"
//  private val userDir = System.getProperty("user.dir")
  private val defaultRootLibFolder = "lib"
  private val logger = Logger

  def findClasses[T](
    packageName: Option[String])(
    implicit m: ClassTag[T]
  ): Traversable[Class[T]] = {
    val reflections = packageName.map(new Reflections(_)).getOrElse(new Reflections())
    val clazz = m.runtimeClass

    reflections.getSubTypesOf(clazz).filter(currentClazz =>
      !currentClazz.isPrimitive && !Modifier.isAbstract(currentClazz.getModifiers) && !currentClazz.isInterface
    ).map(_.asInstanceOf[Class[T]])
  }

  @Deprecated
  def findClasses[T](
    libPrefix: String,
    packageName: Option[String],
    packageFullMatch: Boolean,
    className: Option[String],
    libFolder: Option[String]
  )(implicit m: ClassTag[T]): Stream[Class[T]] = {
    val cls = Thread.currentThread().getContextClassLoader()

    val filteredClassInfos = findClassInfos[T](cls, libPrefix, packageName, packageFullMatch, className, libFolder)

    filteredClassInfos.map(classInfo =>
      Class.forName(classInfo.name, true, cls).asInstanceOf[Class[T]]
    )
  }

  @Deprecated
  private def streamClassInfos(libPrefix: String, libFolder: Option[String]): Stream[ClassInfo] = {
    val defaultClasspath = new File(".")
    logger.info("Searching libs in a default classpath: " + defaultClasspath.getAbsolutePath)

    val libClasspath = new File(libFolder.getOrElse(defaultRootLibFolder))
    logger.info("Searching libs in a custom classpath: " + libClasspath.getAbsolutePath)

//    logger.info("User dir: " + userDir)

    val libFiles = libClasspath.getAbsoluteFile.listFiles

    val classpath : List[File] =
      if (libFiles != null) {
        logger.info(s"Found ${libFiles.length} files in a custom classpath.")

        val extClasspath = libFiles.filter(file =>
          file.isFile && file.getName.startsWith(libPrefix) && file.getName.endsWith(".jar")
        )

        logger.info(s"Found ${extClasspath.length} libs matching a prefix ${libPrefix}.")

        (extClasspath.toSeq ++ Seq(defaultClasspath)).toList
      } else
        List(defaultClasspath)

    ClassFinder(classpath).getClasses
  }

  @Deprecated
  private def findClassInfos[T](
    cls: ClassLoader,
    libPrefix: String,
    packageName: Option[String],
    packageFullMatch: Boolean,
    className: Option[String],
    libFolder: Option[String]
  )(implicit m: ClassTag[T]): Stream[ClassInfo] = {
    val clazz = m.runtimeClass

    val classInfos = streamClassInfos(libPrefix, libFolder)
    logger.info("Class infos found: " + classInfos.size)

    classInfos.filter{ classInfo =>
      val foundClassName = classInfo.name

      // package match
      val packageMatched = packageName.map { packageName =>
        val lastDot = foundClassName.lastIndexOf('.')
        if (lastDot > -1) {
          val foundPackageName = foundClassName.substring(0, lastDot)
          if (packageFullMatch)
            foundPackageName.equals(packageName)
          else
            foundPackageName.startsWith(packageName)
        } else
          false
      }.getOrElse(true)

      try {
        packageMatched &&
        className.map(foundClassName.endsWith(_)).getOrElse(true) &&
        classInfo.isConcrete &&
        !classInfo.isSynthetic &&
        !classInfo.name.contains("$") &&
        clazz.isAssignableFrom(Class.forName(foundClassName, true, cls))
      } catch {
        case _ : ClassNotFoundException => logger.warn("Class not found: " + foundClassName); false
        case _ : ExceptionInInitializerError => logger.warn("Class initialization failed: " + foundClassName); false
        case _ : NoClassDefFoundError => logger.warn("Class def not found: " + foundClassName); false
      }
    }
  }
}