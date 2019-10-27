package org.ada.server.calc.json

import org.ada.server.calc.impl._
import org.incal.core.util.ReflectionUtil.construct2
import org.ada.server.AdaException
import org.ada.server.calc.impl.UniqueDistributionCountsCalc.UniqueDistributionCountsCalcTypePack
import org.ada.server.util.ClassFinderUtil.findClasses

import scala.reflect.runtime.universe._

object JsonInputConverterFactory {

  private val converterClasses = findClasses[JsonInputConverter[_]](Some("org.ada.server.calc"))

  private val converters = converterClasses.map { clazz =>
    construct2[JsonInputConverter[_]](clazz, Nil)
  }

  private val inputConverterMap: Map[Type, JsonInputConverter[_]] =
    converters.filterNot(_.specificUseClass.isDefined).map { converter =>
      converter.inputType -> converter
    }.toMap

  private val specificUseClassConvertersMap: Map[Class[_], Traversable[JsonInputConverter[_]]] =
    converters.filter(_.specificUseClass.isDefined).map { converter =>
      (converter.specificUseClass.get, converter)
    }.groupBy(_._1).map(x => (x._1, x._2.map(_._2))).toMap

  def apply[IN: TypeTag]: JsonInputConverter[IN] =
    applyOption[IN].getOrElse(throwNotFound[IN])

  def applyOption[IN: TypeTag]: Option[JsonInputConverter[IN]] =
    findByInputType[IN](typeOf[IN], inputConverterMap)

  def apply[IN: TypeTag](useInstance: Any): JsonInputConverter[IN] =
    applyOption[IN](useInstance).getOrElse(throwNotFound[IN])

  def applyOption[IN: TypeTag](useInstance: Any): Option[JsonInputConverter[IN]] =
    findByUseInstanceAux[IN](useInstance)

  private def findByUseInstanceAux[IN: TypeTag](useInstance: Any): Option[JsonInputConverter[IN]] = {
    val inputType = typeOf[IN]

    // try first to find by a specific use instance, if it fails search a non-use input types
    findByUseInstance[IN](useInstance, inputType) match {
      case Some(converter) => Some(converter)
      case None => findByInputType[IN](inputType, inputConverterMap)
    }
  }

  private def findByUseInstance[IN: TypeTag](
    useInstance: Any,
    inputType: Type
  ): Option[JsonInputConverter[IN]] =
    specificUseClassConvertersMap.find { case (clazz, _) =>
      clazz.isInstance(useInstance)
    }.flatMap { case (clazz, converters) =>
//      println(useInstance + " matches: " + clazz.getName)
      val typeConverters = converters.map(converter => (converter.inputType, converter))
      findByInputType[IN](inputType, typeConverters)
    }

  private def findByInputType[IN](
    inputType: Type,
    typeConverters: Traversable[(Type, JsonInputConverter[_])]
  ): Option[JsonInputConverter[IN]] =
    typeConverters.find { case (typ, _) =>
      inputType <:< typ
    }.map(
      _._2.asInstanceOf[JsonInputConverter[IN]]
    )

  private def throwNotFound[IN: TypeTag] = {
    val typeName = fullTypeName(typeOf[IN])
    throw new AdaException(s"No json input converter exists for the input type ${typeName}. Create a converter class in the services package and it will be automatically fetched and registered.")
  }

  private def fullTypeName(typ: Type) = {
    val innerTypeNames = if (typ.typeArgs.nonEmpty) {
      s"[${typ.typeArgs.map(_.typeSymbol.fullName).mkString(", ")}]"
    } else ""
    typ.typeSymbol.fullName + innerTypeNames
  }
}

object TestFind extends App {
  val factory = JsonInputConverterFactory

  println("----------------------------")

  println(factory.applyOption[UniqueDistributionCountsCalcTypePack[Any]#IN])
  println(factory.applyOption[UniqueDistributionCountsCalcTypePack[String]#IN])
  println(factory.applyOption[NumericDistributionCountsCalcTypePack#IN])
  println(factory.applyOption[Option[Any]])
  println(factory.applyOption[Option[Double]])
  println(factory.applyOption[Array[NumericDistributionCountsCalcTypePack#IN]])
  println(factory.applyOption[Array[NumericDistributionCountsCalcTypePack#IN]])

  println("----------------------------")

  println(factory.applyOption[NumericDistributionCountsCalcTypePack#IN](NumericDistributionCountsCalc))
  println(factory.applyOption[NumericDistributionCountsCalcTypePack#IN](NumericDistributionCountsCalc))

  println("----------------------------")

  println(factory.applyOption[TupleCalcTypePack[Any, Any]#IN])
  println(factory.applyOption[TupleCalcTypePack[Int, Int]#IN])
  println(factory.applyOption[(Option[Any], Option[Any])])
  println(factory.applyOption[GroupTupleCalcTypePack[String, Any, Any]#IN])
  println(factory.applyOption[GroupTupleCalcTypePack[Any, Any, Any]#IN])
  println(factory.applyOption[GroupTupleCalcTypePack[String, Int, Double]#IN])
}